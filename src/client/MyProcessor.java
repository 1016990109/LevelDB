package client;

import client.HeDb;
import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import file.Key;
import file.Value;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MyProcessor implements Processor {
    HeDb client;
    LogHelper logHelper;

    public MyProcessor() {
        String url = KvStoreConfig.getHdfsUrl();
        try {
            logHelper = new LogHelper(this);
            client = new HeDb(this, new Path("/"), new Configuration());
            logHelper.readLogs();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> get(String key) {
        Key k = new Key();
        k.setRowId(key);
        Value v = new Value();
        try {
            boolean isFound = client.read(k, v);

            if (isFound) {
                ByteArrayInputStream byteInt=new ByteArrayInputStream(v.getData().getBytes());
                ObjectInputStream objInt=new ObjectInputStream(byteInt);
                return (Map)objInt.readObject();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean put(String key, Map<String, String> value) {
        return put(key, value, true);
    }

    public boolean put(String key, Map<String, String> value, boolean writeLog) {
        byte[] bytes = null;
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(os);
            oos.writeObject(value);
            bytes = os.toByteArray();

            Key k = new Key();
            k.setRowId(key);
            Value v = new Value(bytes);

            if (writeLog) {
                logHelper.writeLog(key, value);
            }

            client.write(k, v);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public synchronized boolean batchPut(Map<String, Map<String, String>> records) {
        Iterator<Map.Entry<String,  Map<String, String>>> entries = records.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String,  Map<String, String>> entry = entries.next();
            if (!put(entry.getKey(), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    public byte[] process(byte[] inupt) {
        System.out.println("receive info:" + new String(inupt));
        return "received!".getBytes();
    }

    public void deleteLog() {
        try {
            logHelper.refresh();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
