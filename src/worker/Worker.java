package worker;

import config.Config;
import io.Command;
import io.TaskMessage;
import mapreduce.MapReduce;
import master.FileManager;

import java.io.*;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

/* The worker class which does the processing as orchestered by the Master */
public class Worker extends Thread {

    private final int MONITOR_INTERVAL = 2000;
    private ServerSocket masterConnection;

    //@TODO assign IDs either from master (or use IP / port combo)
    private String WID;
    private int port;
    private File workingDir;
    private ExecutorService executor;
    private final List<Future<?>> tasks;

    private static final int NUM_SELF_THREADS = 4;
    private Map<Command, Map<MapReduce, Map<String, Map<Integer, Future<String>>>>> taskDistribution;

    public static void main(String[] args) {

        int port = Integer.parseInt(args[0]);

        try {
            Worker worker = new Worker(port);
            worker.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Worker(int port) throws IOException {
        this.port = port;
        //masterConnection = new ServerSocket(port);
        executor = Executors.newFixedThreadPool(Math.max(Config.getWorkerThreads(), NUM_SELF_THREADS));
        tasks = Collections.synchronizedList(new ArrayList<Future<?>>());
        WID = String.format("%s:%d", Inet4Address.getLocalHost().getHostAddress(), port);
        taskDistribution = new HashMap<Command, Map<MapReduce, Map<String, Map<Integer, Future<String>>>>>();
    }

    @Override
    public void run() {

        createWorkingDir();
        startMonitor();
        startHeartbeatListener();
        startWorkerListener();
        listen(port);
    }

    private void startWorkerListener() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Socket socket = null;
                try {
                    int port = Worker.this.port + 2;
                    ServerSocket serverSocket = new ServerSocket(port);

                    while (true) {

                        socket = serverSocket.accept();
                        System.out.format("Connected to socket for port %d!\n", port);
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                        try {
                            TaskMessage task = (TaskMessage) in.readObject();

                            if (!task.getCommand().equals(Command.HEARTBEAT)) {
                                System.out.format("Received %s task on port %d!\n", task.getCommand().toString(), port);
                            }

                            handleTask(task, in, out);

                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Error making connection");
                }
            }
        });
    }

    /* Adds a task to its own list.*/
    private void addTask(Command command, MapReduce mapReduce, String filename, int split, Future<String> task) {

        Map<MapReduce, Map<String, Map<Integer, Future<String>>>> m1 = taskDistribution.get(command);
        if (m1 == null) {
            m1 = new ConcurrentHashMap<MapReduce, Map<String, Map<Integer, Future<String>>>>();
            taskDistribution.put(command, m1);
        }
        Map<String, Map<Integer, Future<String>>> m2 = m1.get(mapReduce);
        if (m2 == null) {
            m2 = new ConcurrentHashMap<String, Map<Integer, Future<String>>>();
            m1.put(mapReduce, m2);
        }
        Map<Integer, Future<String>> m3 = m2.get(filename);
        if (m3 == null) {
            m3 = new ConcurrentHashMap<Integer, Future<String>>();
            m2.put(filename, m3);
        }
        m3.put(split, task);

        /*try {
            System.out.format("For task type %s with MapReduce %s, \n" +
                    "\tsplit %d of file %s is being processed on worker at IP %s\n",
                    command, mapReduce, split, filename, Inet4Address.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }*/
    }

    /* Removes task from its list once finished */
    private void removeTask(Command command, MapReduce mapReduce, String filename, int split) {
        taskDistribution.get(command).get(mapReduce).get(filename).remove(split);

        /*try {
            System.out.format("For task type %s with MapReduce %s, \n" +
                    "\tsplit %d of file %s has finished processed on worker at IP %s\n",
                    command, mapReduce, split, filename, Inet4Address.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }*/
    }

    /* Create a directory for the DFS which holds the data to be worked upon */
    private void createWorkingDir() {
        workingDir = new File("worker" + WID);

        if (workingDir.exists()) {
            File[] files = workingDir.listFiles();

            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
            workingDir.delete();
        }

        if (!workingDir.mkdir()) {
            System.out.println("Fuck you, couldn't make working directory.\nI'M GONNA EXIT NOW");
            System.exit(-1);
        }
    }

    /* Starts the heartbeat to let the master know that 'I'm alive!!' */
    private void startHeartbeatListener() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                listen(port + 1);
            }
        });
    }

    /* Listen for incoming messages from the master and execute accordingly */
    private void listen(int port) {
        Socket socket = null;
        try {
            socket = new ServerSocket(port).accept();
            System.out.format("Connected to socket for port %d!\n", port);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            while (true) {
                try {
//                    System.out.println("Waiting for messages...");

                    TaskMessage task = (TaskMessage) in.readObject();

                    if (!task.getCommand().equals(Command.HEARTBEAT)) {
                        System.out.format("Received %s task on port %d!\n", task.getCommand().toString(), port);
                    }

                    handleTask(task, in, out);

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error making connection");
        }
    }

    /* Monitors each task that is being executed on the worker in a monitor thread */
    private void startMonitor() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
//                    synchronized (tasks) {
                        /*for (Future<?> task : tasks) {
                            if (task.isDone()) {
                                tasks.remove(task);
                            }
                        }*/
                        for (Map.Entry<Command, Map<MapReduce, Map<String, Map<Integer, Future<String>>>>> m1
                                : taskDistribution.entrySet()) {

                            for (Map.Entry<MapReduce, Map<String, Map<Integer, Future<String>>>> m2
                                    : taskDistribution.get(m1.getKey()).entrySet()) {

                                for (Map.Entry<String, Map<Integer, Future<String>>> m3
                                        : taskDistribution.get(m1.getKey()).get(m2.getKey()).entrySet()) {

                                    for (Map.Entry<Integer, Future<String>> m4
                                            : taskDistribution.get(m1.getKey()).get(m2.getKey()).get(m3.getKey()).entrySet()) {

                                        if (m4.getValue().isDone()) {

                                            try {
                                                Socket master = new Socket(Config.getMasterIP(), Config.getMasterPort());

                                                ObjectOutputStream out = new ObjectOutputStream(master.getOutputStream());

                                                out.writeObject(m1.getKey());
                                                out.writeObject(m2.getKey());
                                                out.writeObject(m3.getKey());
                                                out.writeObject(m4.getKey());
                                                try {
                                                    out.writeObject(m4.getValue().get());
                                                } catch (Exception e) {
                                                    out.writeObject(e);
                                                }

                                                out.close();

                                                taskDistribution
                                                        .get(m1.getKey())
                                                        .get(m2.getKey())
                                                        .get(m3.getKey())
                                                        .remove(m4.getKey());

                                                System.out.format("Worker at IP %s completed the %s phase of the %s MapReduce task\n" +
                                                        "for split %d of file %s!\n",
                                                        Inet4Address.getLocalHost().getHostAddress(),
                                                        m1.getKey(),
                                                        m2.getKey(),
                                                        m4.getKey(),
                                                        m3.getKey());

                                                removeTask(m1.getKey(), m2.getKey(), m3.getKey(), m4.getKey());

                                            } catch (UnknownHostException e) {
                                                e.printStackTrace();
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        }


                                    }
                                }
                            }
                        }

                        try {
                            //System.out.format("Currently have %d tasks\n", tasks.size());
                            Thread.sleep(MONITOR_INTERVAL);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
//                    }
                }
            }
        });
    }

    private void partitionForReduce(String filename, int numSplits) {

        try {

            File fileToSplit = new File(workingDir + File.separator + filename);
            BufferedReader br = new BufferedReader(new FileReader(
                    fileToSplit));

            List<BufferedWriter> writers = new ArrayList<BufferedWriter>();

            for (int i = 0; i < numSplits; i++) {
                String outName = String.format("%s_%d", filename, i+1);
                outName = workingDir + File.separator + outName;
                BufferedWriter bw = new BufferedWriter(new FileWriter(outName));
                writers.add(bw);
            }

            int size = writers.size();

            String prev = null;
            String curr;

            BufferedWriter curW = null;

            while ((curr = br.readLine()) != null) {
                String[] keyVal = curr.split(" ");
                String key = keyVal[0];
                if (!key.equals(prev)) {
                    curW = writers.get((((key.hashCode() % size) + size) % size));
                    prev = key;
                }
                curW.write(curr);
                curW.newLine();
            }

            for (BufferedWriter bw : writers) {
                bw.close();
            }

            br.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Handle the incoming message and do the work according to the command in the message */
    private void handleTask(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) throws IOException {

        Command command = task.getCommand();

        switch (command) {
            case MAP:
                map(task, in, out);
                break;
            case COMBINE:
                combine(task, in, out);
                break;
            case REDUCE:
                reduce(task, in, out);
                break;
            case HEARTBEAT:
                out.writeObject("\tWorker" + WID + " is stayin' alive\"");
                break;
            case CURRENT_LOAD:
                out.writeObject(getNumTasks());
                break;
            case DOWNLOAD:
                download(task, in, out);
                break;
            case UPLOAD:
                upload(task, in, out);
                break;
            case CLEANUP:
                cleanup(task, in, out);
                break;
            case SHUTDOWN:
                out.writeObject("Shutting down");
                //@TODO cleanup
                System.exit(0);
        }
    }

    private void cleanup(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {
        try {
            String jid = task.getArgs().get("jid");
            if (cleanup(jid)) {
                out.writeObject("Successfully cleaned up files for task " + jid);
            } else {
                out.writeObject("Could not clean up all files for task " + jid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void upload(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {

        try {
            String filename = task.getArgs().get("filename");
            File file = new File(workingDir + File.separator + filename);

            out.writeObject(Integer.toString((int) file.length()));

            System.out.format("Reported a filesize of %d bytes\n", file.length());

            RandomAccessFile rfile = new RandomAccessFile(file, "r");

            String line;
            int bytesSent = 0;

            while ((line = FileManager.readLine(rfile)) != null && !line.isEmpty()) {
                byte[] bytes = line.getBytes();
                out.writeObject(bytes);
                bytesSent += bytes.length;
            }

            System.out.format("Send %d bytes!\n", bytesSent);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void reduce(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {

        try {

            out.writeObject("Got REDUCE task");

            String baseCombineFile = task.getArgs().get("combineFile");
            int splitNum = Integer.parseInt(task.getArgs().get("splitNum"));

            final MapReduce mapReduce = (MapReduce) in.readObject();
            out.writeObject("Got MapReduce object");

            List<String> combineAddresses = new ArrayList<String>();
            List<Integer> combinePorts = new ArrayList<Integer>();

            int numAddresses = (Integer) in.readObject();

            for (int i = 0; i < numAddresses; i++) {
                combineAddresses.add((String) in.readObject());
                combinePorts.add((Integer) in.readObject());
            }

            System.out.format("Received REDUCE task asking me to read split %d of file %s from %d workers\n",
                    splitNum, baseCombineFile, combineAddresses.size());


            Map<String, String> args = new HashMap<String, String>();
            args.put("filename", baseCombineFile + "_" + splitNum);

            List<File> filesForMergesort = new ArrayList<File>();

            for (int i = 0; i < combineAddresses.size(); i++) {
                String inputName = String.format("%s_%s_%d", "PREREDUCE", mapReduce.getName(), i);
                File reduceInput = new File(workingDir + File.separator + inputName);
                FileWriter fw = new FileWriter(reduceInput, true);
                Socket combiner = new Socket(combineAddresses.get(i), combinePorts.get(i));
                ObjectOutputStream cout = new ObjectOutputStream(combiner.getOutputStream());
                ObjectInputStream cin = new ObjectInputStream(combiner.getInputStream());

                cout.writeObject(new TaskMessage(Command.UPLOAD, args));
                int fileSize = Integer.parseInt((String) cin.readObject());

                System.out.format("Expecting %d bytes...\n", fileSize);

                byte[] buffer;
                int numBytesRead = 0;

                while(numBytesRead < fileSize) {
                    buffer = (byte[]) cin.readObject();

                    numBytesRead += buffer.length;

                    String line = new String(buffer);

                    System.out.println(line);
                    System.out.format("Read %d bytes so far...\n", numBytesRead);

                    fw.write(line);
                }

                System.out.format("Done writing %s\n", inputName);

                filesForMergesort.add(reduceInput);

                fw.close();
                cin.close();
            }

            String taskName = "PREREDUCE" + mapReduce.getName();
            File mergesorted = mergeSort(filesForMergesort, 0, taskName);

            System.out.format("PREREDUCE + mergesorted file is %s\n", mergesorted.getName());

            final Map<String, File> partitionedKeys = partitionKeys(mergesorted, taskName);

            Future<String> job = executor.submit(
                new ExecuteReduce(
                    Worker.this,
                    mapReduce,
                    splitNum,
                    new TreeMap<String, File>(partitionedKeys),
                    Command.REDUCE
                )
            );

            addTask(Command.REDUCE, mapReduce, baseCombineFile, splitNum, job);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Get the current number of tasks being run on the worker */
    private int getNumTasks() {

        int numTasks = 0;

        for (Map.Entry<Command, Map<MapReduce, Map<String, Map<Integer, Future<String>>>>> m1
                : taskDistribution.entrySet()) {

            for (Map.Entry<MapReduce, Map<String, Map<Integer, Future<String>>>> m2
                    : taskDistribution.get(m1.getKey()).entrySet()) {

                for (Map.Entry<String, Map<Integer, Future<String>>> m3
                        : taskDistribution.get(m1.getKey()).get(m2.getKey()).entrySet()) {

                    for (Map.Entry<Integer, Future<String>> m4
                            : taskDistribution.get(m1.getKey()).get(m2.getKey()).get(m3.getKey()).entrySet()) {

                        numTasks++;
                    }
                }
            }
         }

        return numTasks;
    }

    /* To combine the results of the Map and organize them to be fed to the reduces */
    private void combine(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {
        try {

            out.writeObject("Got COMBINE task");

            final MapReduce mapReduce = (MapReduce) in.readObject();
            final int numReducers = mapReduce.getNumReducers();

            //System.out.format("Received map task from master:\n\t%s\n", mapReduce.toString());

            out.writeObject("Starting combine task");

            List<File> filesForMergeSort = new ArrayList<File>();

            File[] files = workingDir.listFiles();
            String taskName = mapReduce.getName();
            if (files != null) {
                for (File file : files) {
                    if (file.getName().contains(taskName)) {
                        filesForMergeSort.add(file);
                    }
                }
            }

            //System.out.println("Performing mergesort");

            final File mergesorted = mergeSort(filesForMergeSort, 0, taskName);

            //System.out.println("Finished the mergesort, proceeding with COMBINE");

            final Map<String, File> partitionedKeys = partitionKeys(mergesorted, taskName);

            Future<String> job = executor.submit(
                new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        String result =
                            new ExecuteReduce(
                                Worker.this,
                                mapReduce,
                                    -1, new TreeMap<String, File>(partitionedKeys),
                                Command.COMBINE).call();
                        partitionForReduce(result, mapReduce.getNumReducers());
                        return result;
                    }
                }
            );

            addTask(Command.COMBINE, mapReduce, "", -1, job);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /* Partition the keys to be used further in the processing */
    private Map<String, File> partitionKeys(File mergesorted, String taskName) throws IOException {
        Map<String, File> partitioned = new HashMap<String, File>();
        Map<String, BufferedWriter> streams = new HashMap<String, BufferedWriter>();

        FileReader fr = new FileReader(mergesorted);
        BufferedReader br = new BufferedReader(fr);

        String line;
        int keyNum = 0;

        while ((line = br.readLine()) != null) {
            if (line.split(" ").length == 0) continue;
            String key = line.split(" ")[0];

            File fileForKey = partitioned.get(key);

            if (fileForKey == null) {
                String fileName = String.format("%s_%s_%s", "partition", taskName, key);
                fileForKey = new File(workingDir, fileName);
                partitioned.put(key, fileForKey);

                FileWriter frFK = new FileWriter(fileForKey);
                BufferedWriter brFK = new BufferedWriter(frFK);
                streams.put(key, brFK);
            }

            BufferedWriter brFK = streams.get(key);
            brFK.write(line);
            brFK.newLine();
        }

        for (BufferedWriter brFK : streams.values()) {
            brFK.close();
        }

        return partitioned;
    }

    /* Our very own beloved merge-sort */
    private File mergeSort(List<File> filesForMergeSort, int i, String taskName) throws IOException {

        /*if (filesForMergeSort.size() == 1) return filesForMergeSort.get(0);

        List<File> newlySorted = new ArrayList<File>();


        Iterator<File> files = filesForMergeSort.iterator();

        while (files.hasNext()) {
            File f1 = files.next();
            File f2 = null;

            if (files.hasNext()) {
                f2 = files.next();
            }

            if (f2 == null) {
                File s1 = sort(f1, i, taskName);
                newlySorted.add(s1);
            } else {
                File s1 = sort(f1, i, taskName);
                File s2 = sort(f2, i, taskName);
                File m = merge(s1, s2, ++i, taskName);
                newlySorted.add(m);
            }
        }

        return mergeSort(newlySorted, ++i, taskName);*/

        File mergesorted = new File(String.format("%s_%s", "mergesorted", taskName));
        BufferedWriter bw = new BufferedWriter(new FileWriter(mergesorted));

        List<String> allLines = new ArrayList<String>();

        for (File file : filesForMergeSort) {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                allLines.add(line);
            }
        }

        Collections.sort(allLines);

        for (String line : allLines) {
            bw.write(line);
            bw.newLine();
        }

        bw.close();

        return mergesorted;
    }

    /* Merge the two files and write them out */
    private File merge(File s1, File s2, int i, String taskName) throws IOException {

        FileReader fr1 = new FileReader(s1);
        BufferedReader br1 = new BufferedReader(fr1);
        FileReader fr2 = new FileReader(s2);
        BufferedReader br2 = new BufferedReader(fr2);

        String outName = String.format("%s_%s_%s_%s_%s.txt",
                "merge", taskName, s1.getName(), s2.getName(), i);

        File output = new File(workingDir, outName);

        FileWriter fw = new FileWriter(output);
        BufferedWriter bw = new BufferedWriter(fw);

        String line1;
        String line2 = null;

        while ((line1 = br1.readLine()) != null) {
            while ((line2 = br2.readLine()) != null) {
                if (line1 != null && line1.compareTo(line2) < 0) {
                    bw.write(line1);
                    bw.newLine();
                    line1 = br1.readLine();
                } else {
                    bw.write(line2);
                    bw.newLine();
                }
            }

            bw.write(line1);
            bw.newLine();
        }

        bw.close();

        return output;
    }

    /* Sorts the items from the input file and writes them out */
    private File sort(File file, int i, String taskName) throws IOException {
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);

        List<String> mapped = new ArrayList<String>();

        String outName = String.format("%s_%s_%s.txt", "sort", taskName, i);

        File output = new File(workingDir, outName);

        FileWriter fw = new FileWriter(output);
        BufferedWriter bw = new BufferedWriter(fw);

        String line;

        while ((line = br.readLine()) != null) {
            mapped.add(line);
        }

        Collections.sort(mapped);

        for (String item : mapped) {
            System.out.println(item);
            bw.write(item);
            bw.newLine();
        }

        bw.close();

        return output;
    }

    /* Executes the Map as defined and add that task to our list of running tasks */
    private void map(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {

        try {

            String filename = task.getArgs().get("file");
            int split = Integer.parseInt(task.getArgs().get("split"));

            out.writeObject("got MAP task");

            MapReduce mapReduce = (MapReduce) in.readObject();

            //System.out.format("Received map task from master:\n\t%s\n", mapReduce.toString());

            out.writeObject("Starting map task");

            Future<String> job  = executor.submit(new ExecuteMap(this, mapReduce, filename, split));

            addTask(Command.MAP, mapReduce, filename, split, job);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Download the file and place it in out DFS directory */
    private void download(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {

        try {
            Map<String, String> args = task.getArgs();

            //@TODO put arg names in a static class,
            String fileBaseName = args.get("filename");
            int filePartitionNum = Integer.parseInt(args.get("split"));
            long fileNumBytes = Long.parseLong(args.get("numBytes"));

            //FileOutputStream fos = new FileOutputStream(workingDir + File.separator + fileBaseName + filePartitionNum);
            //BufferedOutputStream bout = new BufferedOutputStream(fos);
            //@TODO:Buffer the writes
            FileWriter fw = new FileWriter(workingDir + File.separator + fileBaseName + "_" + filePartitionNum, true);
            byte[] buffer;
            int numBytesRead = 0;

            while(numBytesRead < fileNumBytes) {
                buffer = (byte[]) in.readObject();

                numBytesRead += buffer.length;

                String line = new String(buffer);

                System.out.println(line);

                fw.write(line);
            }

            fw.close();

            System.out.format("Requested download of:\n" +
                    "\tsplit %d of file %s - %d bytes\n", filePartitionNum, fileBaseName, fileNumBytes);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private boolean cleanup(String jid) {


        File[] files = workingDir.listFiles();

        boolean allDeleted = true;

        if (files != null) {
            for (File file : files) {
                if (match(file, jid)) {
                    allDeleted &= file.delete();
                }
            }
        }

        if (!allDeleted) {
            System.out.println("Could not delete all files!");
        }

        return allDeleted;
    }

    private boolean match(File file, String jid) {
        String name = file.getName();

        if (name == null) return false;

        String combine            = String.format("%s_%s", "COMBINE", jid);
        String combineSplit       = String.format("%s_%s_", "COMBINE", jid);
        String prereducePartition = String.format("%s_%s%s_", "partition", "PREREDUCE", jid);
        String partition          = String.format("%s_%s_", "partition", jid);
        String prereduce          = String.format("%s_%s_", "PREREDUCE", jid);
        String reduce             = String.format("%s_%s_", "REDUCE", jid);
        String map                = String.format("%s_%s_", "MAP", jid);

        return name.matches(reduce + "\\d") &&
               (name.equals(combine) ||
                name.contains(combineSplit) ||
                name.contains(prereducePartition) ||
                name.contains(partition) ||
                name.contains(prereduce) ||
                name.contains(reduce) ||
                name.contains(map));
    }

    File getWorkingDir() {
        return workingDir;
    }
}
