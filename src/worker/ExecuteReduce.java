package worker;

import common.Pair;
import io.Command;
import mapreduce.MapReduce;
import mapreduce.ReduceTask;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;

public class ExecuteReduce implements Callable<String> {

    private Worker worker;
    private MapReduce mapReduce;
    private Map<String, File> keyVals;
    private Command command;

    ExecuteReduce(Worker worker, MapReduce mapReduce, Map<String, File> keyVals, Command command) {
        this.worker = worker;
        this.mapReduce = mapReduce;
        this.keyVals = keyVals;
        this.command = command;
    }

    @Override
    public String call() {

        String delim = mapReduce.getDelim();
        File workingDir = worker.getWorkingDir();

        File outputFile = new File(workingDir, String.format("%s_%s",
                command, mapReduce.getName()));

        try {
            FileWriter fw = new FileWriter(outputFile);

            BufferedWriter bw = new BufferedWriter(fw);

            for (Map.Entry<String, File> e : keyVals.entrySet()) {

                String key = e.getKey();
                File file = e.getValue();

                Scanner scanner = new Scanner(file);


                ReduceTask reduce = mapReduce.getReduce();

                ReduceIterator iterator = new ReduceIterator(scanner);

                Pair<String, String> result = reduce.reduce(key, iterator);

                String resultString = String.format("%s %s%n", result.getX(), result.getY());
                System.out.println(resultString);
                bw.write(resultString);
                bw.newLine();

                bw.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return outputFile.getName();
    }

    private class ReduceIterator implements Iterator<String> {

        private Iterator<String> iterator;

        private ReduceIterator(Iterator<String> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public String next() {
            return iterator.next().split(" ")[0];
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }
}
