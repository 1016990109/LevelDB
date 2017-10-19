package test;

import client.HeDb;
import file.Key;
import file.Value;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;

public class WriterTest {
    public static void main(String[] args) {
        try {
            HeDb client = new HeDb(new Path("/"), new Configuration());

            for (int i = 0; i < 1001; i++) {
                Key key = new Key();
                key.setRowId(i + "");
                HashMap<String, String> map = new HashMap<String, String>();
                map.put(i + "", i + "");
                byte[] bytes = null;
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                ObjectOutputStream oos = null;
                oos = new ObjectOutputStream(os);
                oos.writeObject(map);
                bytes = os.toByteArray();
                Value value = new Value(bytes);
                client.write(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
