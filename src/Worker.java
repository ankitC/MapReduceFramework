import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

public class Worker extends Thread {

    private ServerSocket masterConnection;
    private int WID = -1;
    private File workingDir;
    //private ExecutorService executor;

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
        //executor = Executors.newFixedThreadPool(Config.getWorkerThreads());
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
            case DOWNLOAD:
                download(task, in, out);
                break;
        }
    }

    private void download(TaskMessage task, ObjectInputStream in, ObjectOutputStream out) {

        try {
            Map<String, String> args = task.getArgs();

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

