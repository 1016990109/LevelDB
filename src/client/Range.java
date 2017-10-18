package client;

import file.Key;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Range implements Writable {
  //一个range只会有一个文件，所以这个range要大
  //每一个range只存路径，真正的index在该目录下的index文件中
  private static final BytesWritable EMPTY = new BytesWritable();
  private static final LongWritable ZERO = new LongWritable();
  private Key startingKey;
  private Key endingKey;
  private Path path;
  
  public static void main(String[] args) {
    Range range = new Range(new Path("./cool/000002_000012"));
    System.out.println(range.contains(new Key(new BytesWritable(Utils.toBytesFromHexString("000001")),EMPTY,ZERO)));
    System.out.println(range.contains(new Key(new BytesWritable(Utils.toBytesFromHexString("000002")),EMPTY,ZERO)));
    System.out.println(range.contains(new Key(new BytesWritable(Utils.toBytesFromHexString("00000200")),EMPTY,ZERO)));
    System.out.println(range.contains(new Key(new BytesWritable(Utils.toBytesFromHexString("000011")),EMPTY,ZERO)));
    System.out.println(range.contains(new Key(new BytesWritable(Utils.toBytesFromHexString("000012")),EMPTY,ZERO)));
    System.out.println(range.contains(new Key(new BytesWritable(Utils.toBytesFromHexString("00001200")),EMPTY,ZERO)));
    System.out.println(range.contains(new Key(new BytesWritable(Utils.toBytesFromHexString("000013")),EMPTY,ZERO)));
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    startingKey.write(out);
    endingKey.write(out);
    Utils.writeString(out,path.toString());
  }

  //读单个range
  @Override
  public void readFields(DataInput in) throws IOException {
    if (startingKey == null) {
      startingKey = new Key();
    }
    startingKey.readFields(in);
    if (endingKey == null) {
      endingKey = new Key();
    }
    endingKey.readFields(in);
    path = new Path(Utils.readString(in));
  }

  public Range(Path path) {
    this.path = path;
  }

  public Range(Path path, Key startKey, Key endKey) {
    this.path = path;
    this.startingKey = startKey;
    this.endingKey = endKey;
  }

  public Range() {
    
  }

  public boolean contains(Key key) {
    if (key.compareTo(startingKey) >= 0) {
      if (key.compareTo(endingKey) <= 0) {
        return true;
      }
    }
    return false;
  }

  public Key getStartingKey() {
    return startingKey;
  }

  public Key getEndingKey() {
    return endingKey;
  }

  public Path getPath() {
    return path;
  }

}