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

    private Map<Command, Map<MapReduce, Map<String, Map<Integer, IPAddress>>>> taskDistribution;

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
                        address.getAddress(), completed, mapReduce, split, filename, Command.REDUCE);
                break;
            case COMBINE:
                break;
            case REDUCE:
                break;
            default:
                throw new IllegalStateException("Invalid completion status sent");
        }
    }

    private void initTaskDistribution() {
        taskDistribution = new HashMap<Command, Map<MapReduce, Map<String, Map<Integer, IPAddress>>>>();

        Command[] taskTypes = new Command[]{Command.MAP, Command.COMBINE, Command.REDUCE};

        for (Command c : taskTypes) {
            taskDistribution.put(c, new HashMap<MapReduce, Map<String, Map<Integer, IPAddress>>>());
        }
    }

    private void addTask(Command command, MapReduce mapReduce, String filename, int split, IPAddress address) {

        Map<MapReduce, Map<String, Map<Integer, IPAddress>>> m1 = taskDistribution.get(command);
        if (m1 == null) {
            m1 = new HashMap<MapReduce, Map<String, Map<Integer, IPAddress>>>();
            taskDistribution.put(command, m1);
        }
        Map<String, Map<Integer, IPAddress>> m2 = m1.get(mapReduce);
        if (m2 == null) {
            m2 = new HashMap<String, Map<Integer, IPAddress>>();
            m1.put(mapReduce, m2);
        }
        Map<Integer, IPAddress> m3 = m2.get(filename);
        if (m3 == null) {
            m3 = new HashMap<Integer, IPAddress>();
            m2.put(filename, m3);
        }
        m3.put(split, address);

        System.out.format("For task type %s with MapReduce %s, \n" +
                "\tsplit %d of file %s is being processed on worker at IP %s\n",
                command, mapReduce, split, filename, address.getAddress());
    }

    private void removeTask(Command command, MapReduce mapReduce, int split, IPAddress address) {
        taskDistribution.get(command).get(mapReduce).remove(split);
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

                addTask(Command.MAP, mapReduce, file.getName(), split, a);

                System.out.format("Worker at IP %s responded with: %s\n", a.getAddress(), response);
            }
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
