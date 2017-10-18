package file;

import client.Utils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Key implements WritableComparable<Key> {

  private static final String UTF_8 = "UTF-8";
  private static final BytesWritable EMPTY = new BytesWritable();
  public BytesWritable rowId = new BytesWritable();
  public BytesWritable qualifier = new BytesWritable();
  public LongWritable timestamp = new LongWritable();

  public Key() {

  }

  public Key(BytesWritable rowId, BytesWritable qualifier, LongWritable timestamp) {
    this.rowId.set(rowId);
    this.qualifier.set(qualifier);
    this.timestamp.set(timestamp.get());
  }

  public Key(byte[] rowId, byte[] qualifier, long timestamp) {
    this(new BytesWritable(rowId), new BytesWritable(qualifier), new LongWritable(timestamp));
  }

  public Key(BytesWritable rowId, BytesWritable qualifier, long timestamp) {
    this.rowId.set(rowId);
    this.qualifier.set(qualifier);
    this.timestamp.set(timestamp);
  }

  public static void main(String[] args) {
    List<Key> keys = new ArrayList<Key>();
    keys.add(Key.create("1", "1", 12345));
    keys.add(Key.create("1", "2", 12345));
    keys.add(Key.create("1", "3", 12345));
    keys.add(Key.create("0", "5", 12345));
    keys.add(Key.create("0", "5", 12346));
    keys.add(Key.create("0", "5", 12343));

    Collections.sort(keys);

    for (Key k : keys) {
      System.out.println(k);
    }

    Key a = Key.create("A", "5", 12346);
    Key b = Key.create("B", "5", 12346);
    if (a.compareTo(b) < 0) {
      System.out.println("i get it!");
    }
  }

  private static Key create(String rowId, String qual, long ts) {
    Key key = new Key();
    key.rowId = new BytesWritable(rowId.getBytes());
    key.qualifier = new BytesWritable(qual.getBytes());
    key.timestamp = new LongWritable();
    key.timestamp.set(ts);
    return key;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    rowId.write(out);
    qualifier.write(out);
    timestamp.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    rowId.readFields(in);
    qualifier.readFields(in);
    timestamp.readFields(in);
  }

  @Override
  public int compareTo(Key k) {
    int c1 = rowId.compareTo(k.rowId);
    if (c1 == 0) {
      int c2 = qualifier.compareTo(k.qualifier);
      if (c2 == 0) {
        return k.timestamp.compareTo(timestamp);
      }
    }
    return c1;
  }

  @Override
  public String toString() {
    return new String(rowId.getBytes(), 0, rowId.getLength()) + "/" + qualifier + "/" + timestamp;
  }

  public void set(Key k) {
    rowId.set(k.rowId);
    qualifier.set(k.qualifier);
    timestamp.set(k.timestamp.get());
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

  public void setQualifier(String s) {
    try {
      setQualifier(s.getBytes(UTF_8));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public void setQualifier(byte[] bs) {
    setQualifier(bs, 0, bs.length);
  }

  public void setQualifier(byte[] bs, int offset, int length) {
    qualifier.set(bs, offset, length);
  }

  public static Key newKey(String rowid) {
    try {
      return newKey(rowid.getBytes(UTF_8));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Key newKey(byte[] rowid) {
    return newKey(rowid, 0, rowid.length);
  }

  public static Key newKey(byte[] rowid, int rowidOffset, int rowidLength) {
    BytesWritable rid = new BytesWritable();
    rid.set(rowid, rowidOffset, rowidLength);
    return newKey(rid);
  }

  public static Key newKey(BytesWritable rowid) {
    return newKey(rowid,EMPTY);
  }

  private static Key newKey(BytesWritable rowid, BytesWritable qualifier) {
    return new Key(rowid,qualifier,System.currentTimeMillis());
  }

  public static Key newKey(int i) {
    BytesWritable rowid = new BytesWritable();
    Utils.b(rowid, i);
    return newKey(rowid);
  }

  public static Key newKey(int rowid, int qualifier) {
    BytesWritable rid = new BytesWritable();
    Utils.b(rid, rowid);
    BytesWritable q = new BytesWritable();
    Utils.b(q, qualifier);
    return newKey(rid, q);
  }
}
