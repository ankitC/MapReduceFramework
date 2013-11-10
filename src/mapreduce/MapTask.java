package mapreduce;

import common.Pair;

import java.io.Serializable;
import java.util.List;

public interface MapTask extends Serializable {

    public List<Pair<String, String>> map(String k1);
}
