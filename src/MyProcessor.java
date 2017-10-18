import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MyProcessor implements Processor {
    static{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    Map<String, Map<String, String>> store = new HashMap();

    public MyProcessor() {
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
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(KvStoreConfig.getHdfsUrl()), conf);
            Path mapFile=new Path(key);
            MapFile.Reader reader=new MapFile.Reader(fs,mapFile.toString(),conf);
            Text readKey = new Text();
            Text readValue = new Text();
            reader.next(readKey, readValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public boolean put(String key, Map<String, String> value) {
        return true;
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
}
