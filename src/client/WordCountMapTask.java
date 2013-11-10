package client;

import common.Pair;
import mapreduce.MapTask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WordCountMapTask implements MapTask, Serializable {

    @Override
    public List<Pair<String, String>> map(String k1) {
        List<Pair<String, String>> mapped = new ArrayList<Pair<String, String>>();
        String[] items = k1.split(" ");
        for (String item : items) {
            mapped.add(new Pair<String, String>(item.replaceAll("\\S", ""), Integer.toString(1)));
        }
        return mapped;
    }
}
