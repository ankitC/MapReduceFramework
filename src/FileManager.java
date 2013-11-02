import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileManager {

    private Map<String, Map<Integer, IPAddress>> fileDistribution;

    public FileManager() {
        fileDistribution = new ConcurrentHashMap<String, Map<Integer, IPAddress>>();
    }


}
