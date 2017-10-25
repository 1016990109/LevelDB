package cn.edu.nju.file;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Key implements WritableComparable<Key> {

  private static final String UTF_8 = "UTF-8";
  private static final BytesWritable EMPTY = new BytesWritable();
  public BytesWritable rowId = new BytesWritable();

  public Key() {

  }

  public Key(BytesWritable rowId) {
    this.rowId.set(rowId);
  }

  public Key(byte[] rowId) {
    this(new BytesWritable(rowId));
  }

  private static Key create(String rowId) {
    Key key = new Key();
    key.rowId = new BytesWritable(rowId.getBytes());
    return key;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    rowId.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    rowId.readFields(in);
  }

  @Override
  public int compareTo(Key k) {
    return rowId.compareTo(k.rowId);
  }

  @Override
  public String toString() {
    return new String(rowId.getBytes(), 0, rowId.getLength());
  }

  public void set(Key k) {
    rowId.set(k.rowId);
  }

  public void setRowId(String s) {
    try {
      setRowId(s.getBytes(UTF_8));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public void setRowId(byte[] bs) {
    setRowId(bs, 0, bs.length);
  }

  public void setRowId(byte[] bs, int offset, int length) {
    rowId.set(bs, offset, length);
  }

}
