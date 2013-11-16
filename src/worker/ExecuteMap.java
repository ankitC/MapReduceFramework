package worker;

import common.Pair;
import io.Command;
import mapreduce.MapReduce;
import mapreduce.MapTask;

import java.io.*;
import java.util.List;
import java.util.concurrent.Callable;


/* Executes the maps and stores the results on the local disk to be fed to the reducer */
public class ExecuteMap implements Callable<String> {

    private Worker worker;
    private MapReduce mapReduce;
    private String filename;
    private int split;

    ExecuteMap(Worker worker, MapReduce mapReduce, String filename, int split) {
        this.worker = worker;
        this.mapReduce = mapReduce;
        this.filename = filename;
        this.split = split;
    }

    @Override
    public String call() {

        String delim = mapReduce.getDelim();
        File workingDir = worker.getWorkingDir();

        File outputFile = new File(workingDir, String.format("%s_%s_%s_%s",
                Command.MAP, mapReduce.getName(), filename, split));

        try {
            FileReader fr = new FileReader(new File(workingDir, filename + "_" + split));
            BufferedReader br = new BufferedReader(fr);

            FileWriter fw = new FileWriter(outputFile);
            BufferedWriter bw = new BufferedWriter(fw);

            String line;
            MapTask map = mapReduce.getMap();

            while ((line = br.readLine()) != null) {
                System.out.println(line);
                List<Pair<String, String>> mapped = map.map(line);
                for (Pair<String, String> item : mapped) {
                    String kv = String.format("%s %s", item.getX(), item.getY());
                    bw.write(kv);
                    bw.newLine();
                    System.out.println(kv);
                }
            }

            bw.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return outputFile.getName();
    }
}
