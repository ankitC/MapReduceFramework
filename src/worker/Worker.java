package worker;

import config.Config;
import io.Command;
import io.TaskMessage;
import mapreduce.MapReduce;

import java.io.*;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    }

    @Override
    public void run() {

        createWorkingDir();
        startMonitor();
        startHeartbeatListener();
        listen(port);
    }

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

    private void startHeartbeatListener() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                listen(port + 1);
            }
        });
    }

    private void listen(int port) {
        Socket socket = null;
        try {
            socket = new ServerSocket(port).accept();
            System.out.format("Connected to socket for port %d!\n", port);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            while (true) {
                try {
                    System.out.println("Waiting for messages...");

                    TaskMessage task = (TaskMessage) in.readObject();

                    System.out.format("Received %s task on port %d!\n", task.getCommand().toString(), port);

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

    private void startMonitor() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    synchronized (tasks) {
                        for (Future<?> task : tasks) {
                            if (task.isDone()) {
                                tasks.remove(task);
                            }
                        }

                        try {
                            //System.out.format("Currently have %d tasks\n", tasks.size());
                            Thread.sleep(MONITOR_INTERVAL);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
    }

    private void handleTask(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) throws IOException {

        Command command = task.getCommand();

        switch (command) {
            case MAP:
                map(task, in, out);
                break;
            case COMBINE:
                break;
            case REDUCE:
                break;
            case HEARTBEAT:
                out.writeObject("\tWorker" + WID + " is stayin' alive\"");
                break;
            case CURRENT_LOAD:
                out.writeObject(tasks.size());
                break;
            case DOWNLOAD:
                download(task, in, out);
                break;
            case SHUTDOWN:
                out.writeObject("Shutting down");
                //@TODO cleanup
                System.exit(0);
        }
    }

    private void map(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {

        try {

            out.writeObject("got MAP task");

            MapReduce mapReduce = (MapReduce) in.readObject();

            System.out.format("Received map task from master:\n\t%s\n", mapReduce.toString());

            out.writeObject("Starting map task");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void download(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {

        try {
            Map<String, String> args = task.getArgs();

            //@TODO put arg names in a static class
            String fileBaseName = args.get("filename");
            int filePartitionNum = Integer.parseInt(args.get("split"));
            long fileNumBytes = Long.parseLong(args.get("numBytes"));

            //FileOutputStream fos = new FileOutputStream(workingDir + File.separator + fileBaseName + filePartitionNum);
            //BufferedOutputStream bout = new BufferedOutputStream(fos);
            FileWriter fw = new FileWriter(workingDir + File.separator + fileBaseName + filePartitionNum, true);
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
}
