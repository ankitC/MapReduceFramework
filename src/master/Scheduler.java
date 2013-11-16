package master;

import common.Pair;
import config.Config;
import io.Command;
import io.IPAddress;
import io.TaskMessage;
import mapreduce.MapReduce;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Scheduler {

    private Master master;
    private FileManager fileManager;

    private List<MapReduce> mapTasks;
    private List<MapReduce> combineTasks;
    private List<MapReduce> reduceTasks;

    private Map<String, List<IPAddress>> combineOutputs;

    private Map<String, Map<Command, Map<String, Map<String, List<Integer>>>>> taskDistribution;

    Scheduler(Master master) {
        this.master = master;
        fileManager = master.getFileManager();
        mapTasks = new ArrayList<MapReduce>();
        combineTasks = new ArrayList<MapReduce>();
        reduceTasks = new ArrayList<MapReduce>();
        combineOutputs = new ConcurrentHashMap<String, List<IPAddress>>();
        initTaskDistribution();
    }

    void schedule(Command completed, MapReduce mapReduce,
                  String filename, int split, IPAddress address, String result) throws IOException, ClassNotFoundException {

        String jid = mapReduce.getName();
        switch (completed) {
            case MAP:
                System.out.format("Now that worker at IP %s has finished phase %s of task %s, " +
                        "processing split %d of file %s, we now send it a %s task\n",
                        address, completed, jid, split, filename, Command.COMBINE);

                removeTask(address.getAddress(), completed, mapReduce, filename, split);

                int numMapsLeft = 0;

                for (String file : getFiles(address.getAddress(), completed, mapReduce)) {
                    List<Integer> tasks = getTasks(address.getAddress(), completed, mapReduce, file);
                    if (tasks != null) {
                        numMapsLeft += tasks.size();
                    }

                }

                if (numMapsLeft == 0) {
                    combine(mapReduce, address);
                }

                break;
            case COMBINE:
                System.out.format("Now that worker at IP %s has finished phase %s of task %s, " +
                        "processing split %d of file %s, we now do a global %s task\n",
                        address, completed, jid, split, filename, Command.REDUCE);

                removeTask(address.getAddress(), completed, mapReduce, filename, split);

                if (combineOutputs.get(jid) == null) {
                    List<IPAddress> outputs = new ArrayList<IPAddress>();
                    combineOutputs.put(jid, outputs);
                }
                System.out.format("Adding worker %s to the list of combiners for task %s\n",
                        address.getAddress(), jid);
                combineOutputs.get(jid).add(address);

                int numCombinesLeft = 0;

                for (Map.Entry<String, Map<Command, Map<String, Map<String, List<Integer>>>>> m1 : taskDistribution.entrySet()) {
                    for (Map.Entry<Command, Map<String, Map<String, List<Integer>>>> m2 : m1.getValue().entrySet()) {
                        for (Map.Entry<String, Map<String, List<Integer>>> m3 : m2.getValue().entrySet()) {
                            for (Map.Entry<String, List<Integer>> m4 : m3.getValue().entrySet()) {
                                if (m3.getKey().equals(jid)) {
                                    numCombinesLeft += m4.getValue().size();
                                }
                            }
                        }
                    }
                }

                if (numCombinesLeft == 0) {
                    reduce(mapReduce, result);
                }

                break;
            case REDUCE:
                System.out.println("FINISHED A FREAKING MAPREDUCE TASK OMFUKCINGGEEEEE");
                System.out.format("...so anyways, we finished task %s\n", jid);

                Map<String, String> args = new HashMap<String, String>();
                args.put("filename", result);

                Socket socket = master.getActiveWorkers().get(address);
                int fileNumBytes = Integer.parseInt(master.send(address, socket, Command.UPLOAD, args));

                ObjectInputStream in = master.getActiveInputStreams().get(address);

                FileWriter fw = new FileWriter(result);
                byte[] buffer;
                int numBytesRead = 0;

                while(numBytesRead < fileNumBytes) {
                    buffer = (byte[]) in.readObject();

                    numBytesRead += buffer.length;

                    String line = new String(buffer);

                    System.out.println(line);

                    fw.write(line);
                }

                fw.close();

                File file = new File(result);
                master.getFileManager().writeToDFS(file);

                if (!file.delete()) {
                    System.out.format("Could not delete temp result file for job %s :(\n", jid);
                }

                args.put("jid", jid);

                master.send(address, socket, Command.CLEANUP, args);

                removeTask(address.getAddress(), completed, mapReduce, filename, split);



                break;
            default:
                throw new IllegalStateException("Invalid completion status sent");
        }
    }

    private void initTaskDistribution() {
        taskDistribution = new HashMap<String, Map<Command, Map<String, Map<String, List<Integer>>>>>();
    }

    private void addTask(String address, Command command, MapReduce mapReduce, String filename, int split) {

        Map<Command, Map<String, Map<String, List<Integer>>>> m1 = taskDistribution.get(address);
        if (m1 == null) {
            m1 = new HashMap<Command, Map<String, Map<String, List<Integer>>>>();
            taskDistribution.put(address, m1);
        }
        Map<String, Map<String, List<Integer>>> m2 = m1.get(command);
        if (m2 == null) {
            m2 = new HashMap<String, Map<String, List<Integer>>>();
            m1.put(command, m2);
        }
        Map<String, List<Integer>> m3 = m2.get(mapReduce.getName());
        if (m3 == null) {
            m3 = new HashMap<String, List<Integer>>();
            m2.put(mapReduce.getName(), m3);
        }
        List<Integer> m4 = m3.get(filename);
        if (m4 == null) {
            m4 = new ArrayList<Integer>();
            m3.put(filename, m4);
        }
        m4.add(split);

        /*System.out.format("For task type %s with MapReduce %s, \n" +
                "\tsplit %d of file %s is being processed on worker at IP %s\n",
                command, mapReduce.getName(), split, filename, address);

        System.out.format("Verifying that we correctly added a task:\n" +
                "%s\n%s\n%s\n%s\n%d\n",
                address, command, mapReduce.getName(), filename, split);

        List<Integer> list = taskDistribution.get(address).get(command).get(mapReduce.getName()).get(filename);

        System.out.println((list == null) ? "NOPE IT WASN'T ADDED" : "Yep we're good here");*/
    }

    private void removeTask(String address, Command command,
                            MapReduce mapReduce, String filename, int split) {

        /*System.out.format("Request to remove task:\n" +
                "%s\n%s\n%s" +
                "\n%s\n%d\n",
                address, command, mapReduce.getName(), filename, split);*/

        Map<Command, Map<String, Map<String, List<Integer>>>> commandMapMap = taskDistribution.get(address);
        Map<String, Map<String, List<Integer>>> mapReduceMapMap = commandMapMap.get(command);
        Map<String, List<Integer>> stringListMap = mapReduceMapMap.get(mapReduce.getName());
        List<Integer> integers1 = stringListMap.get(filename);

        List<Integer> integers = taskDistribution.get(address).get(command).get(mapReduce.getName()).get(filename);
        if (integers != null) {
            integers.remove((Integer) split);
        }
    }

    private List<String> getFiles(String address, Command command,
                                  MapReduce mapReduce) {

        return new ArrayList<String>(taskDistribution.get(address).get(command).get(mapReduce.getName()).keySet());
    }

    private List<Integer> getTasks(String address, Command command,
                                   MapReduce mapReduce, String filename) {
        return taskDistribution.get(address).get(command).get(mapReduce.getName()).get(filename);
    }

    void map(MapReduce mapReduce) throws IOException, ClassNotFoundException {
        for (File file : mapReduce.getFiles()) {
            for (int split = 1; split <= Config.getNumSplits(); split++) {
                List<Pair<Integer, IPAddress>> workerLoads = new ArrayList<Pair<Integer, IPAddress>>();

                for (IPAddress worker : fileManager.getFileDistribution().get(file.getName()).get(split)) {
                    int workerLoad = getWorkerLoad(worker);
                    workerLoads.add(new Pair<Integer, IPAddress>(workerLoad, worker));
                    /*System.out.format("Adding worker %s:%d with workload %d\n",
                            worker.getAddress(), worker.getPort(), workerLoad);*/
                }

                Collections.sort(workerLoads);

                IPAddress a = workerLoads.get(0).getY();
                //System.out.format("Looking for worker %s:%d\n", a.getAddress(), a.getPort());
                Socket s = master.getActiveWorkers().get(a);

                System.out.println("Sending worker MAP command");

                Map<String, String> args = new HashMap<String, String>();
                args.put("file", file.getName());
                args.put("split", Integer.toString(split));

                master.send(a, s, Command.MAP, args);

                //System.out.println("Sending the actual MapReduce object");

                String response = master.send(a, s, mapReduce);

                addTask(a.getAddress(), Command.MAP, mapReduce, file.getName(), split);

                //System.out.format("Worker at IP %s responded with: %s\n", a.getAddress(), response);
            }
        }
    }

    private void combine(MapReduce mapReduce, IPAddress address) throws IOException, ClassNotFoundException {
        //System.out.format("getting worker %s:%d\n", address.getAddress(), address.getPort());
        Socket s = master.getActiveWorkers().get(address);

        System.out.println("Sending worker COMBINE command");

        master.send(address, s, Command.COMBINE, (Map<String, String>) null);

        //System.out.println("Sending the actual MapReduce object");

        String response = master.send(address, s, mapReduce);

        addTask(address.getAddress(), Command.COMBINE, mapReduce, "", -1);

        //System.out.format("Worker at IP %s responded with: %s\n", address, response);
    }

    private void reduce(MapReduce mapReduce, String result) throws IOException, ClassNotFoundException {
        //List<IPAddress> rs = new ArrayList<IPAddress>();
        int numRs = Math.min(mapReduce.getNumReducers(),
                (int) Math.ceil((double) master.getActiveWorkers().size() / (double) 4 * (double) 3));

        List<Pair<Integer, IPAddress>> workerLoads = new ArrayList<Pair<Integer, IPAddress>>();

        for (Map.Entry<IPAddress, Socket> worker : master.getActiveWorkers().entrySet()) {
            IPAddress wAddress = worker.getKey();
            int workerLoad = getWorkerLoad(wAddress);
            workerLoads.add(new Pair<Integer, IPAddress>(workerLoad, wAddress));
            /*System.out.format("Adding worker %s with workload %d\n",
                    wAddress, workerLoad);*/
        }

        Collections.sort(workerLoads);

        Iterator<Pair<Integer, IPAddress>> iterator = workerLoads.iterator();

        while (numRs > 0) {
            while (iterator.hasNext()) {
                Pair<Integer, IPAddress> next = iterator.next();
                IPAddress worker = next.getY();
                //rs.add(worker);
                Map<String, String> args = new HashMap<String, String>();
                args.put("combineFile", result);
                args.put("splitNum", Integer.toString(numRs));

                System.out.format("Sending worker %s a REDUCE command!\n", worker.getAddress());

                Socket socket = master.getActiveWorkers().get(worker);
                master.send(worker, socket, Command.REDUCE, args);
                master.send(worker, socket, mapReduce);

                ObjectOutputStream out = master.getActiveOutputStreams().get(worker);

                out.writeObject(combineOutputs.get(mapReduce.getName()).size());

                System.out.format("There are %d combiners for task %s\n", combineOutputs.get(mapReduce.getName()).size(), mapReduce.getName());

                for (IPAddress a : combineOutputs.get(mapReduce.getName())) {
                    out.writeObject(a.getAddress());
                    out.writeObject(a.getPort() + 2);
                }

                if (--numRs <= 0) {
                    break;
                }
            }

            iterator = workerLoads.iterator();
        }
    }

    public List<MapReduce> getMapTasks() {
        return mapTasks;
    }

    public List<MapReduce> getCombineTasks() {
        return combineTasks;
    }

    public List<MapReduce> getReduceTasks() {
        return reduceTasks;
    }

    public Map<String, Map<Command, Map<String, Map<String, List<Integer>>>>> getTaskDistribution() {
        return taskDistribution;
    }

    private int getWorkerLoad(IPAddress worker) {

        int load = 0;

        try {

            /*System.out.format("Sending worker at IP %s a CURRENT_LOAD command\n",
                                worker.getAddress());*/

            master.getActiveOutputStreams().get(worker).writeObject(
                    new TaskMessage(Command.CURRENT_LOAD, null)
            );

            /*System.out.format("Getting current load of worker at IP %s\n",
                                worker.getAddress());*/

            load = (Integer) master.getActiveInputStreams().get(worker).readObject();

            //System.out.println("\tResponded with " + load);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return load;
    }
}

