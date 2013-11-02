import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/* Config variables for the system */
public class Config {
    private static int masterPort = 15000;

    private static String masterIP = "localhost";

    private static int workerThreads = 4;

    private static int replicationFactor = 3;

    private static int numSplits = 4;

    public static Map<String, String> getProperties() {
        return properties;
    }

    public static void setProperties(Map<String, String> properties) {
        Config.properties = properties;
    }

    private static Map<String, String> properties;

    static {
        properties = new HashMap<String, String>();

        System.out.println("Reading properties file...");

        BufferedReader brn;
        try {
            brn = new BufferedReader(new InputStreamReader(
                    new FileInputStream("properties.txt")));

            String line;

            while ((line = brn.readLine()) != null) {
                String[] nc = line.split(":");
                properties.put(nc[0].toLowerCase(), nc[1]);
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String registryPort = getProperty("masterPort");
        String registryIPAddress = getProperty("masterIP");
        String workerThreads = getProperty("workerThreads");
        String replicationFactor = getProperty("replicationFactor");
        String numSplits = getProperty("numSplits");

        if (registryPort != null) {
            setMasterPort(Integer.parseInt(registryPort));
        }

        if (registryIPAddress != null) {
            setMasterIP(registryIPAddress);
        }

        if (workerThreads != null) {
            setWorkerThreads(Integer.parseInt(workerThreads));
        }

        if (replicationFactor != null) {
            setReplicationFactor(Integer.parseInt(replicationFactor));
        }

        if (numSplits != null) {
            setNumSplits(Integer.parseInt(numSplits));
        }

        System.out.println(stringify());
    }

    private static String getProperty(String property) {
        if (property == null) return null;
        return properties.get(property.toLowerCase());
    }

    public static int getMasterPort() {
        return masterPort;
    }
    private static void setMasterPort(int registrySocket) {
        Config.masterPort = registrySocket;
    }
    public static String getMasterIP() {
        return masterIP;
    }
    private static void setMasterIP(String masterIP) {
        Config.masterIP = masterIP;
    }
    public static int getWorkerThreads() {
        return workerThreads;
    }
    private static void setWorkerThreads(int workerThreads) {
        Config.workerThreads = workerThreads;
    }
    public static int getReplicationFactor() {
        return replicationFactor;
    }
    private static void setReplicationFactor(int replicationFactor) {
        Config.replicationFactor = replicationFactor;
    }
    public static int getNumSplits() {
        return numSplits;
    }
    private static void setNumSplits(int numSplits) {
        Config.numSplits = numSplits;
    }

    public static String stringify() {
        return "Config{" +
                "masterPort=" + masterPort +
                ", masterIP=" + masterIP +
                ", workerThreads=" + workerThreads +
                '}';
    }
}
