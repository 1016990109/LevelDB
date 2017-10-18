package test;

import client.HeDb;
import file.Key;
import file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class WriterTest {
    public static void main(String[] args) {
        try {
            HeDb client = new HeDb(new Path("/"), new Configuration());

            for(int i = 1000; i < 2001; i++) {
                Key key = new Key();
                key.setRowId(i + "");
                Value value = new Value();
                value.setData(i + "");
                client.write(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
