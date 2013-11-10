package mapreduce;

import config.Config;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.List;

public class MapReduce implements Serializable {

    private final String delim;

    private final MapTask map;
    private final ReduceTask reduce;

    private final List<File> files;

    private final String resultName;
    private String name;

    public MapReduce(MapTask map, ReduceTask reduce, List<File> files, String resultName, String name) {
        this(map, reduce, files, "", resultName, name);
    }

    public MapReduce(MapTask map, ReduceTask reduce, List<File> files, String delim, String resultName, String name) {

        this.map = map;
        this.reduce = reduce;
        this.files = files;
        this.delim = delim;
        this.resultName = resultName;
        this.name = name;
    }

    public void mapReduce() throws IOException {

        String address = Config.getMasterIP();
        int port = Config.getMasterPort() + 1;

        Socket master = new Socket(address, port);

        ObjectOutputStream out = new ObjectOutputStream(master.getOutputStream());
        out.writeObject(this);

        out.close();
    }

    public String getResultName() {
        return resultName;
    }

    public List<File> getFiles() {
        return files;
    }

    public ReduceTask getReduce() {
        return reduce;
    }

    public MapTask getMap() {
        return map;
    }

    public String getDelim() {
        return delim;
    }

    @Override
    public String toString() {
        return "MapReduce{" +
                "delim='" + delim + '\'' +
                ", map=" + map +
                ", reduce=" + reduce +
                ", files=" + files +
                ", resultName='" + resultName + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }
}
