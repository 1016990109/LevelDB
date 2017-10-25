package cn.edu.nju.myTest;

import cn.edu.nju.MyProcessor;

import java.util.Map;

public class ReaderTest {
    public static void main(String[] args) {
        MyProcessor myProcessor = new MyProcessor();

        Map<String, String> map = myProcessor.get("1999");
        System.out.println("1999" + "=>" + map);
    }
}
