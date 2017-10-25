package cn.edu.nju.myTest;

import cn.edu.nju.MyProcessor;
import cn.edu.nju.file.Key;
import java.util.HashMap;

public class WriterTest {
    public static void main(String[] args) {
        MyProcessor myProcessor = new MyProcessor();

        for (int i = 1005; i < 1008; i++) {
            Key key = new Key();
            key.setRowId(i + "");
            HashMap<String, String> map = new HashMap<String, String>();
            myProcessor.put(i + "", map);
        }
    }
}
