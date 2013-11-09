package mapreduce;

import common.Pair;

import java.io.Serializable;
import java.util.Iterator;

public interface ReduceTask extends Serializable {

    public Pair<String, String> reduce(String k2, Iterator<String> vs);
}
