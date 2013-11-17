package client;

import config.Config;
import mapreduce.MapReduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/* Starting off client on the machine */

public class WordCount {

    public static void main(String[] args) throws IOException {

        List<File> files = new ArrayList<File>();

        File data = new File(Config.getDataDir());

        File[] dataFiles = data.listFiles();

        if (dataFiles != null) {
            files = Arrays.asList(dataFiles);
        }

        MapReduce mapReduce = new MapReduce(
                new WordCountMapTask(),
                new WordCountReduceTask(),
                3,
                files,
                " ",
                "wordCounts.txt",
                "WoRdCoUnTtAsK");

        mapReduce.mapReduce();
    }
}
