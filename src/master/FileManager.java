package master;

import config.Config;
import io.Command;
import io.IPAddress;
import io.TaskMessage;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FileManager {

    private ConcurrentHashMap<String, Map<Integer, List<IPAddress>>> fileDistribution;
    private Master master;

    FileManager(Master master) {
        this.master = master;
        fileDistribution = new ConcurrentHashMap<String, Map<Integer, List<IPAddress>>>();
    }

    ConcurrentHashMap<String, Map<Integer, List<IPAddress>>> getFileDistribution() {
        return fileDistribution;
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

                if (fileDistribution.get(file.getName()) == null) {
                    fileDistribution.put(file.getName(), new HashMap<Integer, List<IPAddress>>());
                }

                Iterator<Map.Entry<IPAddress, Socket>> workers =
                        master.getActiveWorkers().entrySet().iterator();

                BufferedReader r;

                try {
                    r = new BufferedReader(new FileReader(file));
                } catch (FileNotFoundException e) {
                    System.out.format("Could not open file %s for reading.", file.getName());
                    return false;
                }

                int bytesPerSplit = (int) Math.ceil((double) file.length() / (double) Config.getNumSplits());
                long bytesLeft = file.length();

                for (int split = 1; split <= Config.getNumSplits(); split++) {

                    if (fileDistribution.get(file.getName()).get(split) == null) {
                        fileDistribution.get(file.getName()).put(split, new ArrayList<IPAddress>());
                    }

                    int numAssigned = 0;
                    int splitNumBytes = 0;

                    while (numAssigned < Config.getReplicationFactor()) {
                        while (workers.hasNext()) {

                            Map.Entry<IPAddress, Socket> worker = workers.next();

                            //@TODO send partition line-by-line to worker

                            System.out.format("Worker at IP %s will write at most %d bytes\n",
                                    worker.getKey().getAddress(), bytesPerSplit);

                            IPAddress a = worker.getKey();
                            //Socket s = worker.getValue();

                            Map<String, String> args = new HashMap<String, String>();
                            args.put("numBytes", Long.toString(Math.min(bytesLeft, bytesPerSplit)));
                            args.put("filename", file.getName());
                            args.put("split", Integer.toString(split));

                            int bytesWritten = 0;

                            try {
                                master.getActiveOutputStreams().get(a).writeObject(
                                        new TaskMessage(Command.DOWNLOAD, args)
                                );

                                String line;
                                while (bytesWritten < bytesPerSplit &&
                                        (line = r.readLine()) != null) {


                                    master.getActiveOutputStreams().get(a).writeObject(line.getBytes());
                                    bytesWritten += line.getBytes().length;
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                                return false;
                            }

                            System.out.format("Mock sending split %d of file %s to worker at IP %s\n",
                                    split, file.getName(), a);

                            fileDistribution.get(file.getName()).get(split).add(worker.getKey());
                            bytesWritten = 0;

                            splitNumBytes += bytesWritten;

                            if (++numAssigned >= Config.getReplicationFactor()) {
                                break;
                            }
                        }

                        if (numAssigned < Config.getReplicationFactor()) {
                            workers = master.getActiveWorkers().entrySet().iterator();
                        }
                    }

                    splitNumBytes /= Config.getReplicationFactor();
                    bytesLeft -= splitNumBytes;
                }
            }
        }

        return true;
    }
}
