package client;


import common.Pair;
import mapreduce.ReduceTask;

import java.io.Serializable;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class WordLengthReduceTask implements ReduceTask, Serializable {
    @Override
    public Pair<String, String> reduce(String k2, Iterator<String> vs) {
        SortedSet<String> uniqueVals = new TreeSet<String>() ;
        System.out.println(vs.hasNext());
        while (vs.hasNext()) {
            uniqueVals.add(vs.next());
        }

        StringBuilder out = new StringBuilder();

        for (String val : uniqueVals) {
            out.append(val);
            out.append(" ");
        }

        String result = out.toString();

        return new Pair<String, String>(k2, result.trim());
    }

}
