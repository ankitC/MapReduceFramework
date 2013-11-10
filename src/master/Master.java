package master;

import config.Config;
import config.WorkerConfig;
import io.Command;
import io.IPAddress;
import io.TaskMessage;
import mapreduce.MapReduce;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
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

    private Scheduler scheduler;

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
        master.startFileManager();
        master.startScheduler();
        master.startWorkerTaskListener();
        master.startClientListener();
        master.startShell();
    }

    private void startWorkerTaskListener() {

        executor.execute(new Runnable() {
            @Override
            public void run() {

                ServerSocket serverSocket = null;
                try {
                    serverSocket = new ServerSocket(port);
                } catch (IOException e) {
                    e.printStackTrace();
                    shutdown();
                }

                while (true) {
                    Socket socket = null;
                    try {
                        if (serverSocket != null) {
                            socket = serverSocket.accept();
                            System.out.format("Connected to socket for port %d!\n", port);
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                            MapReduce mapReduce = (MapReduce) in.readObject();
                            Command completed = (Command) in.readObject();

                            System.out.format("Worker at IP %s completed the %s phase of the %s MapReduce task!\n",
                                    socket.getInetAddress().getHostAddress(), completed, mapReduce.toString());
                            System.out.println(mapReduce.toString());

                            in.close();

                            scheduler.schedule(mapReduce, completed);

                        } else {
                            shutdown();
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private void startClientListener() {

        executor.execute(new Runnable() {
            @Override
            public void run() {

                ServerSocket serverSocket = null;
                try {
                    serverSocket = new ServerSocket(port + 1);
                } catch (IOException e) {
                    e.printStackTrace();
                    shutdown();
                }

                while (true) {
                    Socket socket = null;
                    try {
                        if (serverSocket != null) {
                            socket = serverSocket.accept();
                            System.out.format("Connected to socket for port %d!\n", port);
                            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                            MapReduce mapReduce = (MapReduce) in.readObject();

                            System.out.println("Received MapReduce task!");
                            System.out.println(mapReduce.toString());

                            in.close();

                            scheduler.schedule(mapReduce, null);

                        } else {
                            shutdown();
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private void startShell() {

        BufferedReader br =
                new BufferedReader(new InputStreamReader(System.in));

        System.out.println();

        while (true) {
            System.out.print("$ master> ");
            handleInput(br);
        }
    }

    private void handleInput(BufferedReader br) {
        try {
            String line = br.readLine();

            if (line.isEmpty()) return;

            String[] tokens = line.split(" ");
            String command = tokens[0];

            if (command.equals("exit")) {
                shutdown();
            } else if (command.equals("list")) {
                if (tokens.length > 1) {
                    if (tokens[1].equals("workers")) {
                        for (IPAddress active : activeWorkers.keySet()) {
                            System.out.format("ACTIVE %s:%d\n", active.getAddress(), active.getPort());
                        }
                        for (IPAddress disconnected : disconnectedWorkers) {
                            System.out.format("DISCONNECTED %s:%d\n",
                                    disconnected.getAddress(), disconnected.getPort());
                        }
                    } else if (tokens[1].equals("files")) {
                        for (String filename : fileManager.getFileDistribution().keySet()) {
                            findFile(filename);
                        }
                    } else if (tokens[1].equals("jobs")) {
                        System.out.println("Not yet implemented");
                    } else {
                        System.out.println("Invalid command, " +
                                "please choose one of (workers, files, jobs) to list");
                    }
                } else {
                    System.out.println("Please specify what to list (workers, files, jobs)");
                }
            } else if (command.equals("find")) {
                if (tokens.length > 2) {
                    if (tokens[1].equals("file")) {
                        findFile(tokens[2]);
                    } else if (tokens[1].equals("job")) {
                        System.out.println("Not yet implemented");
                    } else {
                        System.out.println("Please specify one of (worker, file, job) to find, " +
                                "along with its name / ID");
                    }
                } else {
                    System.out.println("Please specify what to find (worker, file, job) " +
                            "along with its name / ID");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void findFile(String token) {
        Map<Integer,List<IPAddress>> locations =
                fileManager.getFileDistribution().get(token);

        if (locations == null) {
            System.out.println("Sorry, that file does not exist on the DFS");
        } else {
            System.out.format("%s is distribted across the following locations:\n",
                    token);
            for (Map.Entry<Integer, List<IPAddress>> location : locations.entrySet()) {
                Integer split = location.getKey();
                List<IPAddress> addresses = location.getValue();

                for (IPAddress address : addresses) {
                    System.out.format("\tSplit %d at IP %s:%d\n",
                            split, address.getAddress(),address.getPort());
                }
            }
        }
    }

    private void findWorkers() {
        for (IPAddress a : WorkerConfig.workers) {
            Socket main;
            Socket heartbeat;

            IPAddress b = new IPAddress(a.getAddress(), a.getPort() + 1);

            try {
                main = new Socket(a.getAddress(), a.getPort());
                heartbeat = new Socket(b.getAddress(), b.getPort());

                addWorker(a, main, b, heartbeat);

                //String response1 = send(a, main, Command.HEARTBEAT, null);
                String response2 = send(b, heartbeat, Command.HEARTBEAT, (Map<String, String>) null);

                //System.out.format("worker.Worker at IP %s on port %d responded with message %s\n", a.getAddress(), a.getPort(), response1);
                System.out.format("\t\ton port %d responded with message %s\n", b.getPort(), response2);


            } catch (Exception e1) {
                e1.printStackTrace();
                removeWorker(a, b);
            }
        }
    }

    private void startHeartbeat() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {

                    for (Map.Entry<IPAddress, Socket> e : heartbeats.entrySet()) {

                        IPAddress b = e.getKey();

                        IPAddress a = new IPAddress(b.getAddress(), b.getPort() - 1);

                        Socket s = e.getValue();

                        try {
                            send(b, s, Command.HEARTBEAT, (Map<String, String>) null);
                        } catch (Exception e1) {
                            System.out.format("Encountered exception while trying to communicate with worker at IP %s and port %d\n",
                                    b.getAddress(), b.getPort());
                            removeWorker(a, b);
                        }
                    }

                    for (IPAddress a : disconnectedWorkers) {

                        IPAddress b = new IPAddress(a.getAddress(), a.getPort() + 1);

                        try {

                            Socket main = new Socket(a.getAddress(), a.getPort());
                            Socket heartbeat = new Socket(b.getAddress(), b.getPort());

                            send(b, heartbeat, Command.HEARTBEAT, (Map<String, String>) null);

                            addWorker(a, main, b, heartbeat);

                            System.out.format("Reconnected worker at IP %s on port %d!\n", a.getAddress(), a.getPort());

                        } catch (Exception e) {
                            System.out.println("Could not reconnect worker");
                            removeWorker(a, b);
                        }

                    }

                    //System.out.format("%d active workers, %d inactive workers\n", activeWorkers.size(), disconnectedWorkers.size());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        System.out.println("Stopping heartbeats (oh no, a heart attack!");
                        return;
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

    private void startScheduler() {
        scheduler = new Scheduler(this);
    }

    void shutdown() {

        System.out.println("System shutting down...");

        executor.shutdownNow();

        for (Map.Entry<IPAddress, Socket> e : heartbeats.entrySet()) {

            Socket s = e.getValue();
            IPAddress a = e.getKey();

            System.out.format("Attempting to shut down worker at IP %s on port %d...\n", a.getAddress(), a.getPort());

            try {
                send(a, s, Command.SHUTDOWN, (Map<String, String>) null);
                System.out.println("worker shut down");

            } catch (Exception e1) {
                System.out.println("Encountered exception while trying to shut down worker");
            }
        }

        System.out.println("Cleaning up resources...");


        for (ObjectOutputStream o : activeOutputStreams.values()) {
            try {
                o.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Shutdown complete.");
        System.exit(0);
    }

    void addWorker(IPAddress a, Socket main, IPAddress b, Socket heartbeat) throws IOException {
        activeWorkers.put(a, main);
        if (activeOutputStreams.get(a) == null) {
            activeOutputStreams.put(a, new ObjectOutputStream(main.getOutputStream()));
        }
        if (activeInputStreams.get(a) == null) {
            activeInputStreams.put(a,new ObjectInputStream(main.getInputStream()));
        }
        if (activeOutputStreams.get(b) == null) {
            activeOutputStreams.put(b, new ObjectOutputStream(heartbeat.getOutputStream()));
        }
        if (activeInputStreams.get(b) == null) {
            activeInputStreams.put(b,new ObjectInputStream(heartbeat.getInputStream()));
        }
        heartbeats.put(b, heartbeat);
        disconnectedWorkers.remove(a);
    }

    void removeWorker(IPAddress a, IPAddress b) {
        activeWorkers.remove(a);
        activeOutputStreams.remove(a);
        activeInputStreams.remove(a);
        activeOutputStreams.remove(b);
        activeInputStreams.remove(b);
        heartbeats.remove(b);
        disconnectedWorkers.add(a);
    }

    String send(IPAddress a, Socket s, Command c, Map<String, String> args)
            throws IOException, ClassNotFoundException {

        return send(a, s, c, args, String.class);
    }

    <T> T send(IPAddress a, Socket s, Command c, Map<String, String> args,
               Class<T> type) throws IOException, ClassNotFoundException {
        s.setSoTimeout(10000);

        ObjectOutputStream out = activeOutputStreams.get(a);
        out.writeObject(new TaskMessage(c, args));

        ObjectInputStream in = activeInputStreams.get(a);
        T response = type.cast(in.readObject());

        s.setSoTimeout(0);

        return response;
    }

    String send(IPAddress a, Socket s, Object o)
            throws IOException, ClassNotFoundException {

        return send(a, s, o, String.class);
    }

    <T> T send(IPAddress a, Socket s, Object o,
               Class<T> type) throws IOException, ClassNotFoundException {
        s.setSoTimeout(10000);

        ObjectOutputStream out = activeOutputStreams.get(a);
        out.writeObject(o);

        ObjectInputStream in = activeInputStreams.get(a);
        T response = type.cast(in.readObject());

        s.setSoTimeout(0);

        return response;
    }

    FileManager getFileManager() {
        return fileManager;
    }

    Map<IPAddress, Socket> getActiveWorkers() {
        return activeWorkers;
    }

    ConcurrentHashMap<IPAddress, ObjectOutputStream> getActiveOutputStreams() {
        return activeOutputStreams;
    }

    ConcurrentHashMap<IPAddress, ObjectInputStream> getActiveInputStreams() {
        return activeInputStreams;
    }
}
