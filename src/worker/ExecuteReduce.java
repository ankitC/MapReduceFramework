package worker;

import common.Pair;
import io.Command;
import mapreduce.MapReduce;
import mapreduce.ReduceTask;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;


/* Executes the reduce and stores the results to the DFS?? */
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

        System.out.println("here1");

        try {
            FileWriter fw = new FileWriter(outputFile);

            BufferedWriter bw = new BufferedWriter(fw);

            System.out.println("here2");

            for (Map.Entry<String, File> e : keyVals.entrySet()) {

                System.out.println("here3");

                String key = e.getKey();
                File file = e.getValue();

                System.out.println("here4");

                ReduceTask reduce = mapReduce.getReduce();

                ReduceIterator iterator = new ReduceIterator(file);

                System.out.println("here5");

                Pair<String, String> result = reduce.reduce(key, iterator);

                System.out.println("here6");

                String resultString = String.format("%s %s%n", result.getX(), result.getY());
                System.out.println(resultString);
                bw.write(resultString);
                bw.newLine();

                System.out.println("here7");

            }

            System.out.println("here8");

            bw.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return outputFile.getName();
    }

    private class ReduceIterator implements Iterator<String> {

        private BufferedReader reader;
        private String line;

        private ReduceIterator(File file) throws IOException {
            this.reader = new BufferedReader(new FileReader(file));
            line = reader.readLine();
        }

        @Override
        public boolean hasNext() {
            return line != null;
        }

        @Override
        public String next() throws NoSuchElementException {
            if (line == null) throw new NoSuchElementException();
            String next = line;
            try {
                line = reader.readLine();
                if (line == null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return next.split(" ")[1];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException( "Cannot remove" );
        }
    }
}

