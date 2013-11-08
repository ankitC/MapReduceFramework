package master;

import config.Config;
import io.IPAddress;

import java.io.File;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileManager {

    private Map<String, Map<Integer, IPAddress>> fileDistribution;
    private Master master;

    FileManager(Master master) {
        this.master = master;
        fileDistribution = new ConcurrentHashMap<String, Map<Integer, IPAddress>>();
    }

    boolean bootstrap() {

        File fileDir = new File(Config.getDataDir());

        if (!fileDir.exists()) {
            System.out.println("Data directory does not exist!");
            return false;
        }

        File[] files = fileDir.listFiles();

        if (files != null) {
            for (File file : files) {
                System.out.format("File %s has %d lines\n", file.getName(), file.length());

                Iterator<Map.Entry<IPAddress, Socket>> workers =
                        master.getActiveWorkers().entrySet().iterator();

                for (int split = 1; split <= Config.getNumSplits(); split++) {

                    for (int replica = 1; replica <= Config.getReplicationFactor(); replica++) {

                        int numAssigned = 0;

                        while (workers.hasNext()) {
                            Map.Entry<IPAddress, Socket> worker = workers.next();


                        }
                    }
                }
            }
        }

        return true;
    }
}
