package cn.edu.nju.myTest;

import cn.edu.nju.MyProcessor;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.Map;

public class ReaderTest {
    public static CloseableHttpClient httpclient = null;

    public static void main(String[] args) throws InterruptedException {
        String url = "http://localhost:8500/process";

//        Map<String, String> map = myProcessor.get("31032");
//        System.out.println("12" + "=>" + map);

//        for (int i = 0; i < 31245; i++) {
//            if (i % 2 == 1) {
//                sendPost(url, i + "");
//            }
//            Thread.sleep(10);
//        }
        sendPost(url, 177 + "");

    }

    /**
     * 发送get请求
     *
     * @param url 路径
     * @return
     */
    //post请求方法
    public static String sendPost(String url, String data) {
        String response = null;
        try {
            httpclient = HttpClients.createDefault();
            CloseableHttpResponse httpresponse = null;
            try {
                HttpGet httppost = new HttpGet(url);
                httppost.setHeader("key", data);
                httpresponse = httpclient.execute(httppost);
                response = EntityUtils.toString(httpresponse.getEntity());
                if (response.equals("find nothing")) {
                    System.out.println(data);
                }
            } finally {
                if (httpclient != null) {
                    httpclient.close();
                }
                if (httpresponse != null) {
                    httpresponse.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }
}
