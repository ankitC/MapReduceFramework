package mapreduce;

import common.Pair;

import java.io.Serializable;
import java.util.List;

/* Interface to be implemented when defining a Map task */
public interface MapTask extends Serializable {

    public List<Pair<String, String>> map(String k1);
}
