package test;

import client.HeDb;
import file.Key;
import file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;

public class ReaderTest {
    public static void main(String[] args) {
        try {
            HeDb client = new HeDb(new Path("/"), new Configuration());

            Key key = new Key();
            key.setRowId("39999");
            Value value = new Value();
            client.read(key, value);
            ByteArrayInputStream byteInt=new ByteArrayInputStream(value.getData().getBytes());
            ObjectInputStream objInt=new ObjectInputStream(byteInt);
            HashMap<String, String> map = (HashMap)objInt.readObject();
            System.out.println(key + "=>" + map);
//            client.rollback();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
