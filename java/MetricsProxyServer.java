package flume.sqlserver.sink;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class MetricsProxyServer {

    public static void sendSinkCounterData(String url,String data) {
        HttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(url);

        try {

            // 发送请求并获取响应
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity responseEntity = response.getEntity();

            // 设置请求实体
            StringEntity entity = new StringEntity(data);
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-Type", "application/json");      

            // 处理响应
            if (responseEntity != null) {
                String responseString = EntityUtils.toString(responseEntity);
                // 处理响应内容，可以根据实际情况进行逻辑处理
            }

        } catch (Exception e) {
            e.printStackTrace();
            // 处理异常
        } finally {
            // 释放资源
            httpPost.releaseConnection();
        }
    }
}