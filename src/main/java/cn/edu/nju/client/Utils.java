package cn.edu.nju.client;

import org.apache.hadoop.io.BytesWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Utils {

  static final char[] HEX_CHAR_TABLE = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

  public static BytesWritable b(String s) {
    return new BytesWritable(s.getBytes());
  }

  public static void b(byte[] b, int off, long val) {
    b[off + 7] = (byte) (val >>> 0);
    b[off + 6] = (byte) (val >>> 8);
    b[off + 5] = (byte) (val >>> 16);
    b[off + 4] = (byte) (val >>> 24);
    b[off + 3] = (byte) (val >>> 32);
    b[off + 2] = (byte) (val >>> 40);
    b[off + 1] = (byte) (val >>> 48);
    b[off + 0] = (byte) (val >>> 56);
  }
  
  public static void b(byte[] b, int off, int val) {
    b[off + 3] = (byte) (val >>> 0);
    b[off + 2] = (byte) (val >>> 8);
    b[off + 1] = (byte) (val >>> 16);
    b[off + 0] = (byte) (val >>> 24);
  }

  public static BytesWritable b(long val) {
    byte[] b = new byte[8];
    b(b, 0, val);
    return new BytesWritable(b);
  }

  public static void addHexString(StringBuilder builder, byte[] raw, int offset, int length) {
    int len = length + offset;
    for (int i = offset; i < len; i++) {
      int v = raw[i] & 0xFF;
      builder.append(HEX_CHAR_TABLE[v >>> 4]);
      builder.append(HEX_CHAR_TABLE[v & 0xF]);
    }
  }

  public static String toStr(BytesWritable bw) {
    return new String(bw.getBytes(), 0, bw.getLength());
  }

  public static void main(String args[]) throws Exception {
    byte[] byteArray = { (byte) 255, (byte) 254, (byte) 253, (byte) 252, (byte) 251, (byte) 250 };

    StringBuilder builder = new StringBuilder();
    Utils.addHexString(builder, byteArray, 0, 6);

    System.out.println(builder);
    byte[] bs = toBytesFromHexString(builder.toString());
    /*
     * output : fffefdfcfbfa
     */
    for (int i = 0; i < bs.length; i++) {
      System.out.println(bs[i] + " " + byteArray[i]);
    }

  }

  public static byte[] toBytesFromHexString(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static void writeString(DataOutput out, String str) throws IOException {
    byte[] bs = str.getBytes();
    org.apache.hadoop.record.Utils.writeVInt(out, bs.length);
    out.write(bs);
  }

  public static String readString(DataInput in) throws IOException {
    int length = org.apache.hadoop.record.Utils.readVInt(in);
    byte[] buffer = new byte[length];
    in.readFully(buffer);
    return new String(buffer);
  }

  public static long toLong(BytesWritable value) {
    return toLong(value.getBytes(), 0);
  }

  public static long toLong(byte[] b, int off) {
    return ((b[off + 7] & 0xFFL) << 0) + ((b[off + 6] & 0xFFL) << 8) + ((b[off + 5] & 0xFFL) << 16) + ((b[off + 4] & 0xFFL) << 24) + ((b[off + 3] & 0xFFL) << 32)
        + ((b[off + 2] & 0xFFL) << 40) + ((b[off + 1] & 0xFFL) << 48) + (((long) b[off + 0]) << 56);
  }
  
  public static void b(BytesWritable bw, int val) {
    byte[] b;
    if (bw.getCapacity() >= 4) {
      b = bw.getBytes();  
    } else {
      b = new byte[4];
    }
    b(b, 0, val);
    bw.set(b, 0, 4);
  }

  public static void b(BytesWritable bw, long val) {
    byte[] b;
    if (bw.getCapacity() >= 8) {
      b = bw.getBytes();  
    } else {
      b = new byte[8];
    }
    b(b, 0, val);
    bw.set(b, 0, 8);
  }

}