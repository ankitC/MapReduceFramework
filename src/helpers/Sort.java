package helpers;

import java.util.*;

/* Simple Sorter for LinkedHashMaps
 * Written as a part of Map Reduce framework.
 *
 */
public class Sort {


    public static <K extends Comparable<K>, V extends Comparable<V>> Map<K, V> sortHashMapByValues(Map<K, V> passedMap) {
        List<K> mapKeys = new ArrayList<K>(passedMap.keySet());
        List<V> mapValues = new ArrayList<V>(passedMap.values());
        Collections.sort(mapValues);
        Collections.sort(mapKeys);

        LinkedHashMap<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (V val : mapValues) {

            for (K key : mapKeys) {
                String comp1 = passedMap.get(key).toString();
                String comp2 = val.toString();

                if (comp1.equals(comp2)) {
                    passedMap.remove(key);
                    mapKeys.remove(key);
                    sortedMap.put(key, val);
                    break;
                }

            }

        }
        return sortedMap;
    }
}
