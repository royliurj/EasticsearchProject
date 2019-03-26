package com.roy.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @Author: Roy
 * @Date: 2019/3/12 10:35
 */
public class TestCon {

    //服务器地址
    private static String host = "192.168.50.232";
    //端口
    private static int port = 9300;

    public static void main(String[] args) throws UnknownHostException {

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port));

        System.out.println(client);
        client.close();
    }
}
