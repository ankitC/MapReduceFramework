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

    Scheduler(Master master) {
        this.master = master;
        fileManager = master.getFileManager();
    }

    void mapReduce(MapReduce mapReduce) throws IOException, ClassNotFoundException {


        for (File file : mapReduce.getFiles()) {
            for (int split = 1; split <= Config.getReplicationFactor(); split++) {
                List<Pair<Integer, IPAddress>> workerLoads = new ArrayList<Pair<Integer, IPAddress>>();

                for (IPAddress worker : fileManager.getFileDistribution().get(file.getName()).get(split)) {
                    workerLoads.add(new Pair<Integer, IPAddress>(getWorkerLoad(worker), worker));
                }

                Collections.sort(workerLoads);

                IPAddress a = workerLoads.get(0).getY();
                Socket s = master.getActiveWorkers().get(a);

                master.send(a, s, Command.MAP,  (Map<String, String>) null);

                String response = master.send(a, s, mapReduce);

                System.out.format("Worker at IP %s responded with: %s\n", a.getAddress(), response);
            }
        }
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
