package com.roy.es;

import com.google.gson.JsonObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

/**
 * @Author: Roy
 * @Date: 2019/3/12 10:42
 */
public class TestIndex {
    //服务器地址
    private static String host = "192.168.50.232";
    //端口
    private static int port = 9300;
    TransportClient client;

    @Before
    public void getClient() throws Exception{
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port));
    }

    @After
    public void close(){
        if(client != null){
            client.close();
        }
    }

    /**
     * 创建索引
     */
    @Test
    public void testIndex1(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "java编程思想");
        jsonObject.addProperty("publishDate", "2012-01-10");
        jsonObject.addProperty("price", 100);

        IndexResponse indexResponse = client.prepareIndex("book", "java", "1")
                .setSource(jsonObject.toString(), XContentType.JSON).get();
        System.out.println("Index Name:" + indexResponse.getIndex());
        System.out.println("Type:" + indexResponse.getType());
        System.out.println("文档ID：" + indexResponse.getId());
        System.out.println("当前实例的状态：" + indexResponse.status());

        System.out.println(indexResponse.toString());
    }

    /**
     * 根据ID获取文档
     */
    @Test
    public void testGet(){
        GetResponse response = client.prepareGet("book", "java", "1").get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 根据ID修改文档
     */
    @Test
    public void testUpdate(){

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "java编程思想2");
        jsonObject.addProperty("publishDate", "2012-11-10");
        jsonObject.addProperty("price", 102);

        UpdateResponse indexResponse = client.prepareUpdate("book", "java", "1")
                .setDoc(jsonObject.toString(), XContentType.JSON).get();

        System.out.println("Index Name:" + indexResponse.getIndex());
        System.out.println("Type:" + indexResponse.getType());
        System.out.println("文档ID：" + indexResponse.getId());
        System.out.println("当前实例的状态：" + indexResponse.status());

        System.out.println(indexResponse.toString());
    }

    /**
     * 删除文档
     */
    @Test
    public void testDelete(){
        DeleteResponse indexResponse = client.prepareDelete("book", "java", "1").get();


        System.out.println("Index Name:" + indexResponse.getIndex());
        System.out.println("Type:" + indexResponse.getType());
        System.out.println("文档ID：" + indexResponse.getId());
        System.out.println("当前实例的状态：" + indexResponse.status());

        System.out.println(indexResponse.toString());
    }
}
