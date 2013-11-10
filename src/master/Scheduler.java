package master;

import common.Pair;
import config.Config;
import io.Command;
import io.IPAddress;
import io.TaskMessage;
import mapreduce.MapReduce;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class Scheduler {

    private Master master;
    private FileManager fileManager;

    private List<MapReduce> mapTasks;
    private List<MapReduce> combineTasks;
    private List<MapReduce> reduceTasks;

    private Map<IPAddress, Map<Command, Map<MapReduce, Map<String, List<Integer>>>>> taskDistribution;

    Scheduler(Master master) {
        this.master = master;
        fileManager = master.getFileManager();
        mapTasks = new ArrayList<MapReduce>();
        combineTasks = new ArrayList<MapReduce>();
        reduceTasks = new ArrayList<MapReduce>();
        initTaskDistribution();
    }

    void schedule(Command completed, MapReduce mapReduce,
                  String filename, int split, IPAddress address) throws IOException, ClassNotFoundException {

        switch(completed) {
            case MAP:
                System.out.format("Now that worker at IP %s has finished phase %s of task %s, " +
                        "processing split %d of file %s, we now send it a %s task\n",
                        address.getAddress(), completed, mapReduce, split, filename, Command.COMBINE);

                removeTask(address, completed, mapReduce, filename, split);

                int numMapsLeft = 0;

                for (String file : getFiles(address, completed, mapReduce)) {
                    List<Integer> tasks = getTasks(address, completed, mapReduce, filename);
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
                        address.getAddress(), completed, mapReduce, split, filename, Command.REDUCE);

                removeTask(address, completed, mapReduce, filename, split);

                break;
            case REDUCE:
                break;
            default:
                throw new IllegalStateException("Invalid completion status sent");
        }
    }

    private void initTaskDistribution() {
        taskDistribution = new HashMap<IPAddress, Map<Command, Map<MapReduce, Map<String, List<Integer>>>>>();
    }

    private void addTask(IPAddress address, Command command, MapReduce mapReduce, String filename, int split) {

        Map<Command, Map<MapReduce, Map<String, List<Integer>>>> m1 = taskDistribution.get(address);
        if (m1 == null) {
            m1 = new HashMap<Command, Map<MapReduce, Map<String, List<Integer>>>>();
            taskDistribution.put(address, m1);
        }
        Map<MapReduce, Map<String, List<Integer>>> m2 = m1.get(command);
        if (m2 == null) {
            m2 = new HashMap<MapReduce, Map<String, List<Integer>>>();
            m1.put(command, m2);
        }
        Map<String, List<Integer>> m3 = m2.get(mapReduce);
        if (m3 == null) {
            m3 = new HashMap<String, List<Integer>>();
            m2.put(mapReduce, m3);
        }
        List<Integer> m4 = m3.get(filename);
        if (m4 == null) {
            m4 = new ArrayList<Integer>();
            m3.put(filename, m4);
        }
        m4.add(split);

        System.out.format("For task type %s with MapReduce %s, \n" +
                "\tsplit %d of file %s is being processed on worker at IP %s\n",
                command, mapReduce, split, filename, address.getAddress());
    }

    private void removeTask(IPAddress address, Command command,
                            MapReduce mapReduce, String filename, int split) {

        List<Integer> integers = taskDistribution.get(address).get(command).get(mapReduce).get(filename);
        if (integers != null) {
            integers.remove(split);
        }
    }

    private List<String> getFiles(IPAddress address, Command command,
                                  MapReduce mapReduce) {

        return new ArrayList<String>(taskDistribution.get(address).get(command).get(mapReduce).keySet());
    }

    private List<Integer> getTasks(IPAddress address, Command command,
                             MapReduce mapReduce, String filename) {
        return taskDistribution.get(address).get(command).get(mapReduce).get(filename);
    }

    void map(MapReduce mapReduce) throws IOException, ClassNotFoundException {
        for (File file : mapReduce.getFiles()) {
            for (int split = 1; split <= Config.getNumSplits(); split++) {
                List<Pair<Integer, IPAddress>> workerLoads = new ArrayList<Pair<Integer, IPAddress>>();

                for (IPAddress worker : fileManager.getFileDistribution().get(file.getName()).get(split)) {
                    workerLoads.add(new Pair<Integer, IPAddress>(getWorkerLoad(worker), worker));
                }

                Collections.sort(workerLoads);

                IPAddress a = workerLoads.get(0).getY();
                Socket s = master.getActiveWorkers().get(a);

                System.out.println("Sending worker MAP command");

                Map<String, String> args = new HashMap<String, String>();
                args.put("file", file.getName());
                args.put("split", Integer.toString(split));

                master.send(a, s, Command.MAP,  args);

                System.out.println("Sending the actual MapReduce object");

                String response = master.send(a, s, mapReduce);

                addTask(a, Command.MAP, mapReduce, file.getName(), split);

                System.out.format("Worker at IP %s responded with: %s\n", a.getAddress(), response);
            }
        }
    }

    private void combine(MapReduce mapReduce, IPAddress address) throws IOException, ClassNotFoundException {
        Socket s = master.getActiveWorkers().get(address);

        System.out.println("Sending worker COMBINE command");

        master.send(address, s, Command.COMBINE, (Map<String, String>) null);

        System.out.println("Sending the actual MapReduce object");

        String response = master.send(address, s, mapReduce);

        addTask(address, Command.COMBINE, mapReduce, "", -1);

        System.out.format("Worker at IP %s responded with: %s\n", address.getAddress(), response);
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

    private int getWorkerLoad(IPAddress worker) {

        int load = 0;

        try {

            System.out.format("Sending worker at IP %s a CURRENT_LOAD command\n",
                                worker.getAddress());

            master.getActiveOutputStreams().get(worker).writeObject(
                    new TaskMessage(Command.CURRENT_LOAD, null)
            );

            System.out.format("Getting current load of worker at IP %s\n", 
                                worker.getAddress());
            

            load = (Integer) master.getActiveInputStreams().get(worker).readObject();

            System.out.println("\tResponded with " + load);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return load;
    }
}
