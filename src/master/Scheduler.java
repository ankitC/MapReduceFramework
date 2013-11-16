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

    private Map<String, List<IPAddress>> combineOutputs;

    private final Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> taskDistribution;
    private final Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> completedTasks;

    Scheduler(Master master) {
        this.master = master;
        fileManager = master.getFileManager();
        combineOutputs = new ConcurrentHashMap<String, List<IPAddress>>();
        taskDistribution = new ConcurrentHashMap<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>>();
        completedTasks = new ConcurrentHashMap<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>>();
    }

    void schedule(Command completed, MapReduce mapReduce,
                  String filename, int split, IPAddress address, String result) throws IOException, ClassNotFoundException {

        String jid = mapReduce.getName();
        switch (completed) {
            case MAP:
                System.out.format("Now that worker at IP %s has finished phase %s of task %s, " +
                        "processing split %d of file %s, we now send it a %s task\n",
                        address, completed, jid, split, filename, Command.COMBINE);

                removeTask(address.getAddress(), completed, mapReduce, filename, split, taskDistribution);
                addTask(address.getAddress(), completed, mapReduce, filename, split, completedTasks);

                int numMapsLeft = 0;

                for (String file : getFiles(address.getAddress(), completed, mapReduce,taskDistribution)) {
                    List<Integer> tasks = getTasks(address.getAddress(), completed, mapReduce, file, taskDistribution);
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

                removeTask(address.getAddress(), completed, mapReduce, filename, split, taskDistribution);
                addTask(address.getAddress(), completed, mapReduce, filename, split, completedTasks);

                if (combineOutputs.get(jid) == null) {
                    List<IPAddress> outputs = new ArrayList<IPAddress>();
                    combineOutputs.put(jid, outputs);
                }
                System.out.format("Adding worker %s to the list of combiners for task %s\n",
                        address.getAddress(), jid);
                combineOutputs.get(jid).add(address);

                int numCombinesLeft = 0;

                for (Map.Entry<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> m1 : taskDistribution.entrySet()) {
                    for (Map.Entry<MapReduce, Map<Command, Map<String, List<Integer>>>> m2 : m1.getValue().entrySet()) {
                        for (Map.Entry<Command, Map<String, List<Integer>>> m3 : m2.getValue().entrySet()) {
                            for (Map.Entry<String, List<Integer>> m4 : m3.getValue().entrySet()) {
                                if (m2.getKey().getName().equals(jid) && m3.getKey().equals(Command.COMBINE)) {
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

                removeTask(address.getAddress(), completed, mapReduce, filename, split, taskDistribution);
                addTask(address.getAddress(), completed, mapReduce, filename, split, completedTasks);

                int numReducesLeft = 0;

                for (Map.Entry<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> w : taskDistribution.entrySet()) {
                    for (Map.Entry<MapReduce, Map<Command, Map<String, List<Integer>>>> m : w.getValue().entrySet()) {
                        for (Map.Entry<Command, Map<String, List<Integer>>> c : m.getValue().entrySet()) {
                            for (Map.Entry<String, List<Integer>> f : c.getValue().entrySet()) {
                                if (m.getKey().getName().equals(jid) && c.getKey().equals(Command.REDUCE)) {
                                    numReducesLeft += f.getValue().size();
                                }
                            }
                        }
                    }
                }

                System.out.format("%d REDUCE tasks left\n", numReducesLeft);

                if (numReducesLeft == 0) {

                    for (Map.Entry<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> w : completedTasks.entrySet()) {
                        for (Map.Entry<MapReduce, Map<Command, Map<String, List<Integer>>>> m : w.getValue().entrySet()) {
                            for (Map.Entry<Command, Map<String, List<Integer>>> c : m.getValue().entrySet()) {
                                for (Map.Entry<String, List<Integer>> f : c.getValue().entrySet()) {
                                    for (Integer s : f.getValue()) {
                                        if (m.getKey().getName().equals(jid) && c.getKey().equals(Command.REDUCE)) {

                                            String newResult = result.substring(0, result.lastIndexOf("_"));
                                            newResult += "_" + s;
                                            Map<String, String> args = new HashMap<String, String>();
                                            args.put("filename", newResult);

                                            System.out.println("here1");

                                            Socket socket = master.getActiveWorkers().get(address);
                                            int fileNumBytes = Integer.parseInt(master.send(address, socket, Command.UPLOAD, args));

                                            ObjectInputStream in = master.getActiveInputStreams().get(address);

                                            newResult = newResult.replaceFirst("REDUCE", mapReduce.getResultName());
                                            FileWriter fw = new FileWriter(newResult);
                                            byte[] buffer;
                                            int numBytesRead = 0;

                                            while(numBytesRead < fileNumBytes) {
                                                buffer = (byte[]) in.readObject();

                                                numBytesRead += buffer.length;

                                                String line = new String(buffer);

                                                System.out.println(line);

                                                fw.write(line);
                                            }

                                            System.out.format("Read %d bytes!\n", numBytesRead);

                                            fw.close();

                                            File file = new File(newResult);
                                            master.getFileManager().writeToDFS(file);

                                            if (!file.delete()) {
                                                System.out.format("Could not delete temp result file for job %s :(\n", jid);
                                            }

                                            Map<String, String> args2 = new HashMap<String, String>();
                                            args2.put("jid", jid);

                                            //System.out.println(master.send(address, socket, Command.CLEANUP, args2) + " BOOYAH");

                                            for (Map.Entry<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> worker
                                                    : completedTasks.entrySet()) {
                                                worker.getValue().remove(mapReduce);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                }

                break;
            default:
                throw new IllegalStateException("Invalid completion status sent");
        }
    }

    private void addTask(String address, Command command, MapReduce mapReduce, String filename, int split,
                         Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> taskList) {

        Map<MapReduce, Map<Command, Map<String, List<Integer>>>> m1 = taskList.get(address);
        if (m1 == null) {
            m1 = new HashMap<MapReduce, Map<Command, Map<String, List<Integer>>>>();
            taskList.put(address, m1);
        }
        Map<Command, Map<String, List<Integer>>> m2 = m1.get(mapReduce);
        if (m2 == null) {
            m2 = new HashMap<Command, Map<String, List<Integer>>>();
            m1.put(mapReduce, m2);
        }
        Map<String, List<Integer>> m3 = m2.get(command);
        if (m3 == null) {
            m3 = new HashMap<String, List<Integer>>();
            m2.put(command, m3);
        }
        List<Integer> m4 = m3.get(filename);
        if (m4 == null) {
            m4 = new ArrayList<Integer>();
            m3.put(filename, m4);
        }
        m4.add(split);
    }

    private void removeTask(String address, Command command,
                            MapReduce mapReduce, String filename, int split,
                            Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> taskList) {

        List<Integer> integers = taskList.get(address).get(mapReduce).get(command).get(filename);
        if (integers != null) {
            integers.remove((Integer) split);
        }
    }

    private List<String> getFiles(String address, Command command,
                                  MapReduce mapReduce,
                                  Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> taskList) {

        return new ArrayList<String>(taskList.get(address).get(mapReduce).get(command).keySet());
    }

    private List<Integer> getTasks(String address, Command command,
                                   MapReduce mapReduce, String filename,
                                   Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> taskList) {
        return taskList.get(address).get(mapReduce).get(command).get(filename);
    }

    void map(MapReduce mapReduce) throws IOException, ClassNotFoundException {
        for (File file : mapReduce.getFiles()) {
            for (int split = 1; split <= Config.getNumSplits(); split++) {
                map(mapReduce, file, split);
            }
        }
    }

    IPAddress remap(MapReduce mapReduce, String filename, Integer split) throws IOException, ClassNotFoundException {

        IPAddress a = null;

        synchronized (taskDistribution) {
            a = map(mapReduce, filename, split);
            taskDistribution.get(a.getAddress()).get(mapReduce).remove(Command.COMBINE);
        }

        return a;
    }

    IPAddress map(MapReduce mapReduce, String filename, int split) throws IOException, ClassNotFoundException {
        return map(mapReduce, new File(filename), split);
    }

    IPAddress map(MapReduce mapReduce, File file, int split) throws IOException, ClassNotFoundException {
        List<Pair<Integer, IPAddress>> workerLoads = new ArrayList<Pair<Integer, IPAddress>>();

        for (IPAddress worker : fileManager.getFileDistribution().get(file.getName()).get(split)) {
            int workerLoad = getWorkerLoad(worker);
            workerLoads.add(new Pair<Integer, IPAddress>(workerLoad, worker));
        }

        Collections.sort(workerLoads);

        IPAddress a = workerLoads.get(0).getY();
        Socket s = master.getActiveWorkers().get(a);

        System.out.println("Sending worker MAP command");

        Map<String, String> args = new HashMap<String, String>();
        args.put("file", file.getName());
        args.put("split", Integer.toString(split));

        master.send(a, s, Command.MAP, args);

        master.send(a, s, mapReduce);

        addTask(a.getAddress(), Command.MAP, mapReduce, file.getName(), split, taskDistribution);

        return a;
    }

    private void combine(MapReduce mapReduce, IPAddress address) throws IOException, ClassNotFoundException {

        Socket s = master.getActiveWorkers().get(address);

        System.out.println("Sending worker COMBINE command");

        master.send(address, s, Command.COMBINE, (Map<String, String>) null);

        master.send(address, s, mapReduce);

        addTask(address.getAddress(), Command.COMBINE, mapReduce, "", -1, taskDistribution);
    }

    private void reduce(MapReduce mapReduce, String result) throws IOException, ClassNotFoundException {

        int numRs = Math.min(mapReduce.getNumReducers(),
                (int) Math.ceil((double) master.getActiveWorkers().size() / (double) 4 * (double) 3));

        List<Pair<Integer, IPAddress>> workerLoads = new ArrayList<Pair<Integer, IPAddress>>();

        for (Map.Entry<IPAddress, Socket> worker : master.getActiveWorkers().entrySet()) {
            IPAddress wAddress = worker.getKey();
            int workerLoad = getWorkerLoad(wAddress);
            workerLoads.add(new Pair<Integer, IPAddress>(workerLoad, wAddress));
        }

        Collections.sort(workerLoads);

        Iterator<Pair<Integer, IPAddress>> iterator = workerLoads.iterator();

        while (numRs > 0) {
            while (iterator.hasNext()) {
                Pair<Integer, IPAddress> next = iterator.next();
                IPAddress worker = next.getY();
                Map<String, String> args = new HashMap<String, String>();
                args.put("combineFile", result);
                args.put("splitNum", Integer.toString(numRs));

                System.out.format("Sending worker %s a REDUCE command!\n", worker.getAddress());

                Socket socket = master.getActiveWorkers().get(worker);
                master.send(worker, socket, Command.REDUCE, args);
                master.send(worker, socket, mapReduce);

                ObjectOutputStream out = master.getActiveOutputStreams().get(worker);

                out.writeObject(combineOutputs.get(mapReduce.getName()).size());

                for (IPAddress a : combineOutputs.get(mapReduce.getName())) {
                    out.writeObject(a.getAddress());
                    out.writeObject(a.getPort() + 2);
                }

                addTask(worker.getAddress(), Command.REDUCE, mapReduce, result, numRs, taskDistribution);

                if (--numRs <= 0) {
                    break;
                }
            }

            iterator = workerLoads.iterator();
        }
    }

    public Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> getTaskDistribution() {
        return taskDistribution;
    }

    public Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> getCompletedTasks() {
        return completedTasks;
    }

    private int getWorkerLoad(IPAddress worker) {

        int load = 0;

        try {
            master.getActiveOutputStreams().get(worker).writeObject(
                    new TaskMessage(Command.CURRENT_LOAD, null)
            );

            load = (Integer) master.getActiveInputStreams().get(worker).readObject();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return load;
    }
}

