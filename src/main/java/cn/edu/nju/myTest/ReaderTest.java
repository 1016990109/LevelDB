package cn.edu.nju.myTest;

import cn.edu.nju.MyProcessor;

import java.util.Map;

public class ReaderTest {
    public static void main(String[] args) {
        MyProcessor myProcessor = new MyProcessor();

        Map<String, String> map = myProcessor.get("31032");
        System.out.println("12" + "=>" + map);

//        for (int i = 0; i < 30100; i++) {
//            if (myProcessor.get(i+"") == null) {
//                System.out.println(i);
//            }
//        }
    }
}
