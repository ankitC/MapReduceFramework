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
import java.util.*;
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

    private Map<String, Integer> baseWorkerPortMap;

    private Set<IPAddress> disconnectedWorkers;

    private FileManager fileManager;

    private Scheduler scheduler;

    private static int jid;

    private Master() {
        heartbeats = new ConcurrentHashMap<IPAddress, Socket>();
        activeWorkers = new ConcurrentHashMap<IPAddress, Socket>();
        activeOutputStreams = new ConcurrentHashMap<IPAddress, ObjectOutputStream>();
        activeInputStreams = new ConcurrentHashMap<IPAddress, ObjectInputStream>();
        disconnectedWorkers = new ConcurrentSkipListSet<IPAddress>();
        executor = Executors.newCachedThreadPool();
        baseWorkerPortMap = new HashMap<String, Integer>();
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

    /* Listens to the messages from the workers and distributes tasks */
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

                            Command completed = (Command) in.readObject();
                            MapReduce mapReduce = (MapReduce) in.readObject();
                            String filename = (String) in.readObject();
                            Integer split = (Integer) in.readObject();
                            String result = (String) in.readObject();

                            String hostAddress = socket.getInetAddress().getHostAddress();
                            IPAddress address = new IPAddress(
                                    hostAddress,
                                    baseWorkerPortMap.get(hostAddress)
                            );

                            if (scheduler.getTaskDistribution()
                                    .get(hostAddress)
                                    .get(mapReduce)
                                    .get(completed)
                                    .get(filename)
                                    .contains(split)) {

                                System.out.format("Worker at IP %s completed the %s phase of the %s MapReduce task\n" +
                                        "for split %d of file %s!\n",
                                        hostAddress, completed, mapReduce.toString(),
                                        split, filename);
                                System.out.println(mapReduce.toString());
                                scheduler.schedule(completed, mapReduce, filename, split, address, result);
                            } else {
                                System.out.format("Could not find task %s on file %s split %d for job %s in the list" +
                                        "of assigned tasks for worker %s, must have been cancelled. Ignoring.\n",
                                        completed, filename, split, mapReduce.getName(), hostAddress);
                            }

                            in.close();


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

    /* Listens for incoming MapReduce jobs from any client and initilizes the job */
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
                            mapReduce.setName(String.valueOf(jid++));

                            System.out.println("Received MapReduce task!");
                            System.out.println(mapReduce.toString());

                            in.close();

                            scheduler.map(mapReduce);

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

    /* Interactive shell so we know "what's going on!!" */
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
                        for (String job : findAllJobs()) {
                            findJob(job);
                        }
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
                        findJob(tokens[2]);
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

    private List<String> findAllJobs() {
        List<String> jobs = new ArrayList<String>();

        Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> tasks  = scheduler.getTaskDistribution();

        for (Map.Entry<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> address : tasks.entrySet()) {
            for (Map.Entry<MapReduce, Map<Command, Map<String, List<Integer>>>> jid : address.getValue().entrySet()) {
                jobs.add(jid.getKey().getName());
            }
        }

        return jobs;
    }

    private void findJob(String token) {

        Map<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> tasks  = scheduler.getTaskDistribution();

        System.out.format("Job %s is being processed as follows:\n", token);

        for (Map.Entry<String, Map<MapReduce, Map<Command, Map<String, List<Integer>>>>> address : tasks.entrySet()) {
            for (Map.Entry<MapReduce, Map<Command, Map<String, List<Integer>>>> jid : address.getValue().entrySet()) {
                for (Map.Entry<Command, Map<String, List<Integer>>> command : jid.getValue().entrySet()) {
                    if (jid.getKey().getName().equals(token)) {
                        System.out.format("\t%s on worker %s\n", command.getKey(), address.getKey());
                    }
                }
            }
        }
    }

    /* helper method to find the file */
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

    /* Helper method to find the workers */
    private void findWorkers() {
        for (IPAddress a : WorkerConfig.workers) {
            Socket main;
            Socket heartbeat;

            baseWorkerPortMap.put(a.getAddress(), a.getPort());

            IPAddress b = new IPAddress(a.getAddress(), a.getPort() + 1);

            try {
                main = new Socket(a.getAddress(), a.getPort());
                heartbeat = new Socket(b.getAddress(), b.getPort());

                addWorker(a, main, b, heartbeat);

                send(b, heartbeat, Command.HEARTBEAT, (Map<String, String>) null);

            } catch (Exception e1) {
                e1.printStackTrace();
                removeWorker(a, b);
            }
        }
    }

    /* Start heartbeats to know who is alive and who's a deadman!!*/
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
                            rescheduleJobs(a);
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

    private void rescheduleJobs(IPAddress worker) {
        try {
            for (Map.Entry<MapReduce, Map<Command, Map<String, List<Integer>>>> mapReduce
                    : scheduler.getTaskDistribution().get(worker.getAddress()).entrySet()) {
                for (Map.Entry<Command, Map<String, List<Integer>>> command : mapReduce.getValue().entrySet()) {
                    for (Map.Entry<String, List<Integer>> filename : command.getValue().entrySet()) {
                        for (Integer split : filename.getValue()) {
                            switch (command.getKey()) {
                                case MAP:
                                    scheduler.map(mapReduce.getKey(), filename.getKey(), split);
                                case COMBINE:
                                    for (Map.Entry<MapReduce, Map<Command, Map<String, List<Integer>>>> m
                                            : scheduler.getCompletedTasks().get(worker.getAddress()).entrySet()) {
                                        for (Map.Entry<Command, Map<String, List<Integer>>> c : m.getValue().entrySet()) {
                                            for (Map.Entry<String, List<Integer>> f : c.getValue().entrySet()) {
                                                for (Integer s : f.getValue()) {
                                                    scheduler.remap(m.getKey(), f.getKey(), s);
                                                }
                                            }
                                        }
                                    }
                                    break;
                                case REDUCE:
                                    break;
                                default:
                                    throw new IllegalStateException("Invalid task status");
                            }
                        }
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Starts the file manager and bootstraps the file system */
    private void startFileManager() {
        fileManager = new FileManager(this);

        if (!fileManager.bootstrap()) {
            System.out.println("FATAL: Filesystem bootstrapping failed, cannot recover.\n" +
                    "Shutting down.");
            shutdown();
            System.exit(-1);
        }
    }

    /* Start the scheduler!, the the fun begin!! */
    private void startScheduler() {
        scheduler = new Scheduler(this);
    }

    /* Get the whole system to shut down gracefully, rather, send a "HEART ATTACK!!" *evil laugh* */
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

    /* Add the worker to the books so that we can assign some work to it later */
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

    /* Remove the worker if it is dead and let's not give it work for the time being */
    void removeWorker(IPAddress a, IPAddress b) {
        activeWorkers.remove(a);
        activeOutputStreams.remove(a);
        activeInputStreams.remove(a);
        activeOutputStreams.remove(b);
        activeInputStreams.remove(b);
        heartbeats.remove(b);
        disconnectedWorkers.add(a);
    }

    /* Methods to exchange messages */
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

    Scheduler getScheduler() {
        return scheduler;
    }

    public String send(String a, Socket s, Command command, Map<String, String> args) throws IOException, ClassNotFoundException {
        IPAddress address = new IPAddress(a, baseWorkerPortMap.get(a));
        return send(address, s, command, args);
    }

    public String send(String address, Socket s, MapReduce mapReduce) throws IOException, ClassNotFoundException {
        IPAddress a = new IPAddress(address, baseWorkerPortMap.get(address));
        return send(a, s, mapReduce);
    }
}

