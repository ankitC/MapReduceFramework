package client;


import common.Pair;
import mapreduce.MapTask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/* Finds the length of the word per word basis */
public class WordLengthMapTask implements MapTask, Serializable {

    public List<Pair<String, String>> map(String k1) {
        List<Pair<String, String>> mapped = new ArrayList<Pair<String, String>>();
        String[] items = k1.split(" ");
        for (String item : items) {
            String[] subitems = item.split("-");
            for (String subitem : subitems) {
                mapped.add(new Pair<String, String>(
                        String.valueOf(subitem.length()),
                        subitem
                                .toLowerCase()
                                .replaceAll("[\\s\\.,\\(\\)\']", "")));
            }
        }
        return mapped;
    }

}
