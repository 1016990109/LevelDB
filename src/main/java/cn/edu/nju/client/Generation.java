package cn.edu.nju.client;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Generation {

    public List<Range> ranges = new ArrayList<Range>();

    //单独的range
    public void write(DataOutput out, Range range) throws IOException {
        range.write(out);
    }

    //读单个的range
    public void readFields(DataInput in) throws IOException {
        Range range = new Range();
        range.readFields(in);
        ranges.add(range);
        IOUtils.closeStream((FSDataInputStream) in);
    }

}
