package test;

import client.HeDb;
import client.MyProcessor;
import file.Key;
import file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

public class ReaderTest {
    public static void main(String[] args) {
        MyProcessor myProcessor = new MyProcessor();

        Map<String, String> map = myProcessor.get("1999");
        System.out.println("1999" + "=>" + map);
    }
}
