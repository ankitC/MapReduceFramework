import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileManager {

    private Map<String, Map<Integer, IPAddress>> fileDistribution;
    private Master master;

    public FileManager(Master master) {
        fileDistribution = new ConcurrentHashMap<String, Map<Integer, IPAddress>>();
        this.master = master;
    }

    private void bootstrap() {

        File fileDir = new File(Config.getDataDir());

        if (!fileDir.exists()) {
            System.out.println("Data directory does not exist!");
            master.shutdown();
        }
    }
}
