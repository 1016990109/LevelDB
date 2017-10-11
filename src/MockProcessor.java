import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MockProcessor implements Processor {
    static{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    Map<String, Map<String, String>> store = new HashMap();

    public MockProcessor() {
        String url = KvStoreConfig.getHdfsUrl();
        InputStream in=null;
        try {
            in=new URL(url).openStream();
            IOUtils.copyBytes(in, System.out, 2048,false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(in);
        }
    }

    public Map<String, String> get(String key) {
        Map<String, String> table = (Map)this.store.get(key);
        return table;
    }

    public boolean put(String key, Map<String, String> value) {
        this.store.put(key, value);
        return true;
    }

    public Map<String, Map<String, String>> filter(Map<String, String> filter) {
        Map<String, Map<String, String>> res = new HashMap();
        Iterator var3 = this.store.keySet().iterator();

        while(var3.hasNext()) {
            String key = (String)var3.next();
            if (this.test(filter, (Map)this.store.get(key))) {
                res.put(key, this.store.get(key));
            }
        }

        return res;
    }

    public int count(Map<String, String> filter) {
        Map<String, Map<String, String>> res = this.filter(filter);
        return res.size();
    }

    public synchronized boolean batchPut(Map<String, Map<String, String>> records) {
        Map var2 = this.store;
        synchronized(this.store) {
            this.store.putAll(records);
            return true;
        }
    }

    public byte[] process(byte[] inupt) {
        System.out.println("receive info:" + new String(inupt));
        return "received!".getBytes();
    }

    public boolean test(Map<String, String> filter, Map<String, String> record) {
        Iterator var3 = filter.keySet().iterator();

        String key;
        do {
            if (!var3.hasNext()) {
                return true;
            }

            key = (String)var3.next();
        } while(((String)filter.get(key)).equals(record.get(key)));

        return false;
    }
}
