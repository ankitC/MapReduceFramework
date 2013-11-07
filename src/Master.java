import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Master {

    private int port = Config.getMasterPort();

    private ExecutorService executor;

    private Map<IPAddress, Socket> activeWorkers;
    private Set<IPAddress> disconnectedWorkers;

    private Master() {
        activeWorkers = new ConcurrentHashMap<IPAddress, Socket>();
        disconnectedWorkers = new ConcurrentSkipListSet<IPAddress>();
        executor = Executors.newCachedThreadPool();
    }

    public static void main(String[] args) {

        System.out.println("Master starting...");

        Master master = new Master();

        System.out.println("Attempting to contact workers...");

        //master.findWorkers();
        master.startHeartbeat();
    }

    private void startHeartbeat() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {

                    for (Map.Entry<IPAddress, Socket> e : activeWorkers.entrySet()) {

                        IPAddress a = e.getKey();
                        Socket s;

                        try {
                            s = new Socket(a.getAddress(), a.getPort() + 1);
                            s.setSoTimeout(10000);

                            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                            out.writeObject(new TaskMessage(Command.HEARTBEAT, null));

                            ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                            String response = (String) in.readObject();

                            s.setSoTimeout(0);

                        } catch (Exception e1) {
                            System.out.format("Encountered exception while trying to communicate with worker at IP %s and port %d\n",
                                    a.getAddress(), a.getPort());
                            activeWorkers.remove(a);
                            disconnectedWorkers.add(a);
                        }
                    }

                    System.out.format("%d active workers, %d inactive workers\n", activeWorkers.size(), disconnectedWorkers.size());

                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private void findWorkers() {
        for (IPAddress a : WorkerConfig.workers) {
            Socket s;

            try {
                s = new Socket(a.getAddress(), a.getPort());
                s.setSoTimeout(10000);

                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                out.writeObject(new TaskMessage(Command.HEARTBEAT, null));

                ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                String response = (String) in.readObject();

                s.setSoTimeout(0);

                System.out.format("Worker at IP %s on port %d responded with message %s\n", a.getAddress(), a.getPort(), response);

                activeWorkers.put(a, s);

            } catch (UnknownHostException e1) {
                e1.printStackTrace();
            } catch (IOException e1) {
                e1.printStackTrace();
            } catch (ClassNotFoundException e1) {
                e1.printStackTrace();
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

                }
            }
        }
    }
}
