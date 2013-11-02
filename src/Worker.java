import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Worker extends Thread {

    private ServerSocket masterConnection;
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
        while (true) {
            try {
                System.out.println("Waiting to connect to master...");
                Socket master = masterConnection.accept();

                System.out.println("Connected to master!");

                ObjectOutputStream out = new ObjectOutputStream(master.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(master.getInputStream());

                TaskMessage task = (TaskMessage) in.readObject();

                System.out.format("Received %s task from master!\n", task.getCommand().toString());

                handleTask(task, out);

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleTask(TaskMessage task, ObjectOutputStream out) throws IOException {

        Command command = task.getCommand();

        switch (command) {
            case MAP:
                break;
            case COMBINE:
                break;
            case REDUCE:
                break;
            case HEARTBEAT:
                out.writeObject("\"stayin' alive\"");
                break;
        }
    }
}
