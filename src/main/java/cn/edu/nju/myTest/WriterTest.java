package cn.edu.nju.myTest;

import cn.edu.nju.MyProcessor;
import cn.edu.nju.file.Key;
import java.util.HashMap;

public class WriterTest {
    public static void main(String[] args) {
        MyProcessor myProcessor = new MyProcessor();

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 30100; i < 40003; i++) {
                    Key key = new Key();
                    key.setRowId(i + "");
                    HashMap<String, String> map = new HashMap<String, String>();
                    map.put("col" + i, "val" + i);
                    myProcessor.put(i + "", map);
                }
            }
        });
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 10000; i < 20000; i++) {
                    Key key = new Key();
                    key.setRowId(i + "");
                    HashMap<String, String> map = new HashMap<String, String>();
                    map.put("col" + i, "val" + i);
                    myProcessor.put(i + "", map);
                }
            }
        });
        Thread thread3 = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 20000; i < 30100; i++) {
                    Key key = new Key();
                    key.setRowId(i + "");
                    HashMap<String, String> map = new HashMap<String, String>();
                    map.put("col" + i, "val" + i);
                    myProcessor.put(i + "", map);
                }
            }
        });

        thread1.start();
//        thread2.start();
//        thread3.start();
    }
}
