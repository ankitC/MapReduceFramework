package client;

import common.Pair;
import mapreduce.MapTask;

import java.io.Serializable;

public class WordCountMapTask implements MapTask, Serializable {

    @Override
    public Pair<String, String> map(String k1) {
        return new Pair<String, String>(k1, Integer.toString(1));
    }
}
