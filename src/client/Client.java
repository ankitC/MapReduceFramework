package client;

import config.Config;
import mapreduce.MapReduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Client {

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
                files,
                " ",
                "wordCounts.txt",
                "WoRdCoUnTtAsK");

        mapReduce.mapReduce();
    }
}
