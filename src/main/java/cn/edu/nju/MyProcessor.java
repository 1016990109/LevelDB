package cn.edu.nju;

import cn.edu.nju.client.Info;
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
import java.util.*;

public class MyProcessor implements Processor {
    LevelDB client;
    LogHelper logHelper;
    int serversNum;
    int current;

    public MyProcessor() {
        String url = KvStoreConfig.getHdfsUrl();
        try {
            Configuration configuration = new Configuration();
//            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            logHelper = new LogHelper(this, LevelDB.maxBufferedElements);
            client = new LevelDB(this, new Path("/"), configuration, url);
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
        serversNum = KvStoreConfig.getServersNum();
        current = RpcServer.getRpcServerId();
        try {
            boolean isInCache = client.findInCache(k, v);
            if (!isInCache) {
                ArrayList<Integer> needInformClientIds = new ArrayList<>();
                ArrayList<Integer> notConnectedClientIds = new ArrayList<>();
                for (int i = 0; i < serversNum; i++) {
                    if (i != current) {
                        //send to other kvpod
                        if (RpcServer.isConnected(i)) {
                            needInformClientIds.add(i);
                        } else {
                            notConnectedClientIds.add(i);
                        }
                    }
                }

                //query data
                for (int i = 0; i < needInformClientIds.size(); i++) {
                    try {
                        byte[] result = null;
                        result = RpcClientFactory.inform(needInformClientIds.get(i), info2bytes(new Info(Info.READ, key.getBytes())));
                        Info info = bytes2Info(result);

                        if (info != null && info.getInfo() != null) {
                            return formatBytes(info.getInfo());
                        }

                        //query data from other client
                        if (notConnectedClientIds.size() != 0) {
                            for (int j = 0; j < notConnectedClientIds.size(); j++) {
                                result = RpcClientFactory.inform(needInformClientIds.get(i), info2bytes(new Info(Info.FETCH, (notConnectedClientIds.get(j) + "," + key).getBytes())));
                                info = bytes2Info(result);

                                if (info != null && info.getInfo() != null) {
                                    return formatBytes(info.getInfo());
                                }
                            }
                        }
                    } catch (IOException e) {
                        //connection refused
                        System.out.println("unknown error");
                        e.printStackTrace();
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
        } catch (Exception e) {
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
        Iterator<Map.Entry<String, Map<String, String>>> entries = records.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Map<String, String>> entry = entries.next();
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
        return new HashMap<>();
    }

    public byte[] process(byte[] input) {
        Info receiveInfo = bytes2Info(input);
        if (receiveInfo != null && receiveInfo.getType() == Info.READ) {
            //find key
            return findInCache(receiveInfo);
        } else if (receiveInfo != null && receiveInfo.getType() == Info.RANGE) {
            String[] rangeInfo = new String(receiveInfo.getInfo()).split(",");
            Key startKey = new Key();
            Key endKey = new Key();
            startKey.setRowId(rangeInfo[0]);
            endKey.setRowId(rangeInfo[1]);
            client.updateRange(startKey, endKey, new Path(rangeInfo[2]), false);
            return input;
        } else if (receiveInfo != null && receiveInfo.getType() == Info.FETCH){
            String[] queryInfo = new String(receiveInfo.getInfo()).split(",");
            try {
                return RpcClientFactory.inform(Integer.parseInt(queryInfo[0]), info2bytes(new Info(Info.READ, queryInfo[1].getBytes())));
            } catch (IOException e) {
                e.printStackTrace();
                return input;
            }
        }

        return null;
    }

    private byte[] findInCache(Info receiveInfo) {
        Key k = new Key();
        k.setRowId(new String(receiveInfo.getInfo()));
        Value v = new Value();
        Info info = new Info();
        info.setType(Info.READ);

        boolean isInCache = client.findInCache(k, v);
        if (isInCache) {
            info.setInfo(v.getData().getBytes());
        }
        return info2bytes(info);
    }

    private Map<String, String> formatBytes(byte[] input) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteInt = new ByteArrayInputStream(input);
        ObjectInputStream objInt = new ObjectInputStream(byteInt);
        return (Map) objInt.readObject();
    }

    private byte[] info2bytes(Info info) {
        byte[] bytes = {};
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(info);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return bytes;
    }

    private Info bytes2Info(byte[] bytes) {
        Info info = null;
        try {
            ByteArrayInputStream byteInt = new ByteArrayInputStream(bytes);
            ObjectInputStream objInt = new ObjectInputStream(byteInt);
            info = (Info) objInt.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return info;
    }

    public void notifyAddRange(Key startKey, Key endKey, Path dataPath) {
        informOtherNodes(new Info(Info.RANGE, new StringBuilder()
                .append(new String(startKey.rowId.getBytes()))
                .append(",")
                .append(new String(endKey.rowId.getBytes()))
                .append(",")
                .append(dataPath.toString()).toString().getBytes()));
    }

    private void informOtherNodes(Info info) {
        serversNum = KvStoreConfig.getServersNum();
        current = RpcServer.getRpcServerId();
        for (int i = 0; i < serversNum; i++) {
            if (i != current) {
                try {
                    RpcClientFactory.inform(i, info2bytes(info));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void updateFlushPath(String currentPath) {
        client.updateFlushPath(currentPath);
    }
}
