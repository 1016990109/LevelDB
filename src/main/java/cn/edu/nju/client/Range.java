package cn.edu.nju.client;

import cn.edu.nju.file.Key;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Range implements Writable {
  private Key startingKey;
  private Key endingKey;
  private Path path;
  
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