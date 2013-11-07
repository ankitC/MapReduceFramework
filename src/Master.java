import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Master {

    private int port = Config.getMasterPort();

    private ExecutorService executor;

    private Map<IPAddress, Socket> heartbeats;
    private Map<IPAddress, Socket> activeWorkers;
    private Set<IPAddress> disconnectedWorkers;

    private Master() {
        heartbeats = new ConcurrentHashMap<IPAddress, Socket>();
        activeWorkers = new ConcurrentHashMap<IPAddress, Socket>();
        disconnectedWorkers = new ConcurrentSkipListSet<IPAddress>();
        executor = Executors.newCachedThreadPool();
    }

    public static void main(String[] args) {

        System.out.println("Master starting...");

        Master master = new Master();

        System.out.println("Attempting to contact workers...");

        master.findWorkers();
        master.startHeartbeat();
    }

    private void startHeartbeat() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {

                    for (Map.Entry<IPAddress, Socket> e : heartbeats.entrySet()) {

                        IPAddress a = e.getKey();
                        Socket s = e.getValue();

                        try {

                            send(a, s, Command.HEARTBEAT, null, String.class);

                        } catch (Exception e1) {
                            System.out.format("Encountered exception while trying to communicate with worker at IP %s and port %d\n",
                                    a.getAddress(), a.getPort());
                            heartbeats.remove(a);
                            activeWorkers.remove(a);
                            disconnectedWorkers.add(a);
                        }
                    }

                    for (IPAddress a : disconnectedWorkers) {

                        try {
                            Socket s = new Socket(a.getAddress(), a.getPort() + 1);
                            send(a, s, Command.HEARTBEAT, null, String.class);
                            if (activeWorkers.get(a) == null) {
                                activeWorkers.put(a, new Socket(a.getAddress(), a.getPort()));
                                heartbeats.put(a, s);
                                disconnectedWorkers.remove(a);
                                System.out.format("Reconnected worker at IP %s on port %d!\n", a.getAddress(), a.getPort());
                            }
                        } catch (IOException e) {
//                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
//                            e.printStackTrace();
                        }

                    }

                    System.out.format("%d active workers, %d inactive workers\n", activeWorkers.size(), disconnectedWorkers.size());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private <T> T send(IPAddress a, Socket s, Command c, Map<String, String> args,
                      Class<T> type) throws IOException, ClassNotFoundException {
        s.setSoTimeout(10000);

        ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
        out.writeObject(new TaskMessage(Command.HEARTBEAT, null));

        ObjectInputStream in = new ObjectInputStream(s.getInputStream());
        T response = type.cast(in.readObject());

        s.setSoTimeout(0);

        return response;
    }

    private void findWorkers() {
        for (IPAddress a : WorkerConfig.workers) {
            Socket s1;
            Socket s2;

            try {
                s1 = new Socket(a.getAddress(), a.getPort());
                s2 = new Socket(a.getAddress(), a.getPort() + 1);
                /*s1.setSoTimeout(10000);
                s2.setSoTimeout(10000);

                ObjectOutputStream out1 = new ObjectOutputStream(s1.getOutputStream());
                ObjectOutputStream out2 = new ObjectOutputStream(s2.getOutputStream());
                out1.writeObject(new TaskMessage(Command.HEARTBEAT, null));
                out2.writeObject(new TaskMessage(Command.HEARTBEAT, null));

                ObjectInputStream in1 = new ObjectInputStream(s1.getInputStream());
                ObjectInputStream in2 = new ObjectInputStream(s2.getInputStream());
                String response1 = (String) in1.readObject();
                String response2 = (String) in2.readObject();

                s1.setSoTimeout(0);
                s2.setSoTimeout(0);*/

                String response1 = send(a, s1, Command.HEARTBEAT, null,String.class);
                String response2 = send(a, s2, Command.HEARTBEAT, null,String.class);

                System.out.format("Worker at IP %s on port %d responded with message %s\n", a.getAddress(), a.getPort(), response1);
                System.out.format("\t\ton port %d responded with message %s\n", a.getPort(), response2);

                activeWorkers.put(a, s1);
                heartbeats.put(a, s2);

            } catch (Exception e1) {
                e1.printStackTrace();
                disconnectedWorkers.add(a);
            }
        }
    }

    private void shutdown() {

        System.out.println("System shutting down...");

        for (Map.Entry<IPAddress, Socket> e : activeWorkers.entrySet()) {

            Socket s = e.getValue();
            IPAddress a = e.getKey();

            System.out.format("Attempting to shut down worker at IP %s on port %d...\n", a.getAddress(), a.getPort());

            try {

                s.setSoTimeout(10000);

                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                out.writeObject(new TaskMessage(Command.SHUTDOWN, null));

                ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                String response = (String) in.readObject();

                s.setSoTimeout(0);

                System.out.println("Worker shut down");

            } catch (Exception e1) {
                System.out.println("Encountered exception while trying to shut down worker");
            }
        }

        //@TODO cleanup

        System.out.println("Shutdown complete.");
        System.exit(0);
    }

    private class FileManager {

        private Map<String, Map<Integer, IPAddress>> fileDistribution;

        private FileManager() {
            fileDistribution = new ConcurrentHashMap<String, Map<Integer, IPAddress>>();
        }

        private void bootstrap() {

            File fileDir = new File(Config.getDataDir());

            if (!fileDir.exists()) {
                System.out.println("Data directory does not exist!");
                Master.this.shutdown();
            }

            File[] files = fileDir.listFiles();

            if (files != null) {
                for (File file : files) {
                    System.out.format("File %s has %d lines\n", file.getName(), file.length());

                    Iterator<Map.Entry<IPAddress, Socket>> workers = activeWorkers.entrySet().iterator();

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
        }
    }
}

