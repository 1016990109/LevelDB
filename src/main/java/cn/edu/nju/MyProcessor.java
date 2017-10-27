package cn.edu.nju;

import cn.edu.nju.client.LogHelper;
import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import cn.edu.nju.file.Key;
import cn.edu.nju.file.Value;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MyProcessor implements Processor {
    LevelDB client;
    LogHelper logHelper;

    public MyProcessor() {
        String url = KvStoreConfig.getHdfsUrl();
        try {
            logHelper = new LogHelper(this, LevelDB.maxBufferedElements);
            client = new LevelDB(this, new Path("/"), new Configuration(), url);
            logHelper.readLogs();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized Map<String, String> get(String key) {
        Key k = new Key();
        k.setRowId(key);
        Value v = new Value();
        try {
            boolean isInCache = client.findInCache(k, v);
            if (!isInCache) {
                int serversNum = KvStoreConfig.getServersNum();
                int current = RpcServer.getRpcServerId();

                for (int i =0; i < serversNum; i++) {
                    if (i != current) {
                        //send to other kvpod
                        try {
                            byte[] result = RpcClientFactory.inform(i, key.getBytes());

                            if (result != null) {
                                return formatBytes(result);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            } else {
                return formatBytes(v.getData().getBytes());
            }

            //find in hdfs
            boolean isFound = client.read(k, v);

            if (isFound) {
                return formatBytes(v.getData().getBytes());
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
            } else {
                logHelper.countIncrease();
            }

            client.write(k, v);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean batchPut(Map<String, Map<String, String>> records) {
        Iterator<Map.Entry<String,  Map<String, String>>> entries = records.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String,  Map<String, String>> entry = entries.next();
            if (!put(entry.getKey(), entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int count(Map<String, String> map) {
        return 0;
    }

    @Override
    public Map<Map<String, String>, Integer> groupBy(List<String> list) {
        return null;
    }

    public byte[] process(byte[] input) {
        System.out.println("receive info:" + new String(input));
        Key k = new Key();
        k.setRowId(new String(input));
        Value v = new Value();

        boolean isInCache = client.findInCache(k, v);
        if (isInCache) {
            return v.getData().getBytes();
        } else {
            return null;
        }
    }

    private Map<String, String> formatBytes(byte[] input) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteInt=new ByteArrayInputStream(input);
        ObjectInputStream objInt=new ObjectInputStream(byteInt);
        return (Map)objInt.readObject();
    }

    public void deleteLog() {
        try {
            logHelper.deleteLog();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
