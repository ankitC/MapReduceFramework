import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;

public class WorkerConfig {

    public static Map<String, Integer> workers = new HashMap<String, Integer>();

    static {
        System.out.println("Reading worker configuration file...");

        BufferedReader brn;
        try {
            brn = new BufferedReader(new InputStreamReader(
                    new FileInputStream("worker-config.txt")));

            String line;

            while ((line = brn.readLine()) != null) {
                String[] nc = line.split(":");
                workers.put(nc[0], Integer.parseInt(nc[1]));
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
