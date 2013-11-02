import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Master {

    private int port = Config.getMasterPort();

    private Set<Socket> workers;

    private Master() {
        workers = new HashSet<Socket>();
    }

    public static void main(String[] args) {

        System.out.println("Master starting...");

        Master master = new Master();

        System.out.println("Attempting to contact workers...");

        master.connect();
    }

    private void connect() {
        for (Map.Entry<String, Integer> e : WorkerConfig.workers.entrySet()) {
            Socket s;

            try {
                s = new Socket(e.getKey(), e.getValue());
                s.setSoTimeout(10000);

                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                out.writeObject(new TaskMessage(Command.HEARTBEAT, null));

                ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                String response = (String) in.readObject();

                System.out.format("Worker at IP %s on port %d responded with message %s\n", e.getKey(), e.getValue(), response);

                workers.add(s);

            } catch (UnknownHostException e1) {
                e1.printStackTrace();
            } catch (IOException e1) {
                e1.printStackTrace();
            } catch (ClassNotFoundException e1) {
                e1.printStackTrace();
            }
        }
    }
}
