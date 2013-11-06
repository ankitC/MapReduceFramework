import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Worker extends Thread {

    private final int MONITOR_INTERVAL = 2000;
    private ServerSocket masterConnection;

    //@TODO assign IDs either from master (or use IP / port combo)
    private int WID = -1;
    private File workingDir;
    private ExecutorService executor;
    private final List<Future<?>> tasks;

    private static final int NUM_SELF_THREADS = 2;

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
        masterConnection = new ServerSocket(port);
        executor = Executors.newFixedThreadPool(Math.max(Config.getWorkerThreads(), NUM_SELF_THREADS));
        tasks = Collections.synchronizedList(new ArrayList<Future<?>>());
    }

    @Override
    public void run() {

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


        Socket socket = null;
        try {
            socket = masterConnection.accept();
            System.out.println("Connected to socket!");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error making connection");
        }

        startMonitor();

        while (true) {
            try {
                System.out.println("Waiting for messages...");

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                TaskMessage task = (TaskMessage) in.readObject();

                System.out.format("Received %s task from socket!\n", task.getCommand().toString());

                handleTask(task, in, out);

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
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
                            System.out.format("Currently have %d tasks\n", tasks.size());
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
                break;
            case COMBINE:
                break;
            case REDUCE:
                break;
            case HEARTBEAT:
                out.writeObject("\"Worker " + WID + " is stayin' alive\"");
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

    private void download(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {

        try {
            Map<String, String> args = task.getArgs();

            //@TODO put arg names in a static class
            String fileBaseName = args.get("filename");
            String filePartitionNum = args.get("partition");

            FileOutputStream fos = new FileOutputStream(workingDir + File.separator + fileBaseName + filePartitionNum);
            BufferedOutputStream bout = new BufferedOutputStream(fos);
            byte[] buffer = new byte[1024];
            int count;
            while((count=in.read(buffer)) >= 0){
                bout.write(buffer,0, count);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

