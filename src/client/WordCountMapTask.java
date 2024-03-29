package client;

import common.Pair;
import mapreduce.MapTask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/* The Map task for doing word count */
public class WordCountMapTask implements MapTask, Serializable {

    @Override
    public List<Pair<String, String>> map(String k1) {
        List<Pair<String, String>> mapped = new ArrayList<Pair<String, String>>();
        String[] items = k1.split(" ");
        for (String item : items) {
            String[] subitems = item.split("-");
            for (String subitem : subitems) {
                mapped.add(new Pair<String, String>(
                        subitem
                                .toLowerCase()
                                .replaceAll("[\\s\\.,\\(\\)\']", ""),
                        Integer.toString(1)));
            }
        }
        return mapped;
    }
}
