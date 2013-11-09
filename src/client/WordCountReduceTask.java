package client;

import common.Pair;
import mapreduce.ReduceTask;

import java.io.Serializable;
import java.util.Iterator;

public class WordCountReduceTask implements ReduceTask, Serializable {

    @Override
    public Pair<String, String> reduce(String k2, Iterator<String> vs) {
        int count = 0;

        while (vs.hasNext()) {
            count += Integer.parseInt(vs.next());
        }

        return new Pair<String, String>(k2, Integer.toString(count));
    }
}
