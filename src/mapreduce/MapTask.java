package mapreduce;

import common.Pair;

import java.io.Serializable;

public interface MapTask extends Serializable {

    public Pair<String, String> map(String k1);
}
