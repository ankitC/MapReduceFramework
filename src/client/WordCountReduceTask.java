package client;

import common.Pair;
import mapreduce.ReduceTask;

import java.io.Serializable;
import java.util.Iterator;

/* The Reducer for doing word count */
public class WordCountReduceTask implements ReduceTask, Serializable {

    @Override
    public Pair<String, String> reduce(String k2, Iterator<String> vs) {
        int count = 0;
        System.out.println("fuck");
        System.out.println(vs.hasNext());
        try{while (vs.hasNext()) {
            int i = Integer.parseInt(vs.next());
            System.out.println(i);
            count += i;
        }}catch(Exception e){e.printStackTrace();}
        System.out.println("you");
        return new Pair<String, String>(k2, Integer.toString(count));
    }
}
