package master;

import config.Config;
import config.WorkerConfig;
import io.Command;
import io.IPAddress;
import io.TaskMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
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
    private ConcurrentHashMap<IPAddress, ObjectOutputStream> activeOutputStreams;
    private ConcurrentHashMap<IPAddress, ObjectInputStream> activeInputStreams;


    private Set<IPAddress> disconnectedWorkers;

    private FileManager fileManager;

    private Master() {
        heartbeats = new ConcurrentHashMap<IPAddress, Socket>();
        activeWorkers = new ConcurrentHashMap<IPAddress, Socket>();
        activeOutputStreams = new ConcurrentHashMap<IPAddress, ObjectOutputStream>();
        activeInputStreams = new ConcurrentHashMap<IPAddress, ObjectInputStream>();
        disconnectedWorkers = new ConcurrentSkipListSet<IPAddress>();
        executor = Executors.newCachedThreadPool();
    }

    public static void main(String[] args) {

        System.out.println("master.Master starting...");

        Master master = new Master();

        System.out.println("Attempting to contact workers...");

        master.findWorkers();
        master.startHeartbeat();
    }

    private void findWorkers() {
        for (IPAddress a : WorkerConfig.workers) {
            Socket main;
            Socket heartbeat;

            try {
                main = new Socket(a.getAddress(), a.getPort());
                heartbeat = new Socket(a.getAddress(), a.getPort() + 1);

                String response1 = send(a, main, Command.HEARTBEAT, null, String.class);
                String response2 = send(a, heartbeat, Command.HEARTBEAT, null, String.class);

                System.out.format("worker.Worker at IP %s on port %d responded with message %s\n", a.getAddress(), a.getPort(), response1);
                System.out.format("\t\ton port %d responded with message %s\n", a.getPort(), response2);

                /*activeWorkers.put(a, s1);
                heartbeats.put(a, s2);*/

                addWorker(a, main, heartbeat);

            } catch (Exception e1) {
                e1.printStackTrace();
                removeWorker(a);
            }
        }
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
                            removeWorker(a);
                        }
                    }

                    for (IPAddress a : disconnectedWorkers) {

                        try {
                            Socket main = new Socket(a.getAddress(), a.getPort());
                            Socket heartbeat = new Socket(a.getAddress(), a.getPort() + 1);

                            send(a, heartbeat, Command.HEARTBEAT, null, String.class);
                            if (activeWorkers.get(a) == null) {
                                addWorker(a, main, heartbeat);
                                System.out.format("Reconnected worker at IP %s on port %d!\n", a.getAddress(), a.getPort());
                            }
                        } catch (Exception e) {
                            removeWorker(a);
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

    private void startFileManager() {
        fileManager = new FileManager(this);

        if (!fileManager.bootstrap()) {
            System.out.println("FATAL: Filesystem bootstrapping failed, cannot recover.\n" +
                    "Shutting down.");
            shutdown();
            System.exit(-1);
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

                System.out.println("worker.Worker shut down");

            } catch (Exception e1) {
                System.out.println("Encountered exception while trying to shut down worker");
            }
        }

        //@TODO cleanup

        System.out.println("Shutdown complete.");
        System.exit(0);
    }

    private void addWorker(IPAddress a, Socket main, Socket heartbeat) throws IOException {
        activeWorkers.put(a, main);
        activeOutputStreams.put(a, new ObjectOutputStream(main.getOutputStream()));
        activeInputStreams.put(a,new ObjectInputStream(main.getInputStream()));
        heartbeats.put(a, heartbeat);
        disconnectedWorkers.remove(a);
    }

    private void removeWorker(IPAddress a) {
        activeWorkers.remove(a);
        activeOutputStreams.remove(a);
        activeInputStreams.remove(a);
        heartbeats.remove(a);
        disconnectedWorkers.add(a);
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

    Map<IPAddress, Socket> getActiveWorkers() {
        return activeWorkers;
    }
}

