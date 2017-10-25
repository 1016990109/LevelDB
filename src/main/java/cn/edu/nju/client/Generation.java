package cn.edu.nju.client;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Generation implements Writable {

  public List<Range> ranges = new ArrayList<Range>();

  //写所有的range
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(ranges.size());
    for (Range range : ranges) {
      range.write(out);
    }
  }

  //读所有的range
  @Override
  public void readFields(DataInput in) throws IOException {
    //这里应该是不需要清空，直接append
//    ranges.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      Range range = new Range();
      range.readFields(in);
      ranges.add(range);
    }

    IOUtils.closeStream((FSDataInputStream)in);
  }

}
