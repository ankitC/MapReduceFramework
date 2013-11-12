package config;

import io.IPAddress;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/* Configuration class and parameter initilization for the worker nodes */
public class WorkerConfig {

    public static List<IPAddress> workers = new ArrayList<IPAddress>();

    static {
        System.out.println("Reading worker configuration file...");

        BufferedReader brn;
        try {
            brn = new BufferedReader(new InputStreamReader(
                    new FileInputStream("worker-config.txt")));

            String line;

            while ((line = brn.readLine()) != null) {
                String[] nc = line.split(":");
                workers.add(new IPAddress(nc[0], Integer.parseInt(nc[1])));
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
