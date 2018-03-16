package cn.edu.nju.myTest;

import cn.edu.nju.MyProcessor;
import cn.edu.nju.client.NoHeaderObjectOutputStream;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class NewTest {
    public static void main(String[] args) {
         String logPath = "src/log.out";
         ObjectOutputStream writer;
        try {
            HashMap<String, String> map = new HashMap<>();
            map.put("1", "2");
            File file = new File(logPath);
            FileOutputStream fos = new FileOutputStream(file, true);
            if (file.length() > 0) {
                writer = new NoHeaderObjectOutputStream(fos);
            } else {
                writer = new ObjectOutputStream(fos);
            }
            ObjectOutputStream obj = new ObjectOutputStream(fos);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        write1(obj, map);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        write2(obj, map);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }).start();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void write1(ObjectOutputStream obj, Map<String, String> map) throws IOException {
        System.out.println("1");
        obj.writeObject(new String("1"));
        obj.writeObject(map);
    }

    public static void write2(ObjectOutputStream obj, Map<String, String> map) throws IOException {
        System.out.println("2");
        obj.writeObject(new String("1"));
        obj.writeObject(map);
    }
}
