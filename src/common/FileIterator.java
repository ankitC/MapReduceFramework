package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<String> {

    private BufferedReader reader;
    private String line;

    public FileIterator(File file) throws IOException {
        this.reader = new BufferedReader(new FileReader(file));
        line = reader.readLine();
    }

    @Override
    public boolean hasNext() {
        return line != null;
    }

    @Override
    public String next() throws NoSuchElementException {
        if (line == null) throw new NoSuchElementException();
        String next = line;
        try {
            line = reader.readLine();
            if (line == null) {
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return next.split(" ")[1];
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException( "Cannot remove" );
    }
}
