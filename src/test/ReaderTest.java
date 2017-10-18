package test;

import client.HeDb;
import file.Key;
import file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ReaderTest {
    public static void main(String[] args) {
        try {
            HeDb client = new HeDb(new Path("/"), new Configuration());

            Key key = new Key();
            key.setRowId("578");
            Value value = new Value();
            client.read(key, value);
            System.out.println(key + "=>" + value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
