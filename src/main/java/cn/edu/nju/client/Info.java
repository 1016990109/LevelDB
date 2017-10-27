package cn.edu.nju.client;

import java.io.Serializable;

public class Info implements Serializable {
    public static final int READ = 0;
    public static final int RANGE = 1;

    private int type;
    private byte[] info;

    public Info() {

    }

    public Info (int type, byte[] info) {
        this.type = type;
        this.info = info;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public byte[] getInfo() {
        return info;
    }

    public void setInfo(byte[] info) {
        this.info = info;
    }
}
