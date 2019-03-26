package com.roy.es.employee;

import com.google.gson.JsonObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author: Roy
 * @Date: 2019/3/26 15:50
 */
public class TestApp {
    //服务器地址
    private static String host = "localhost";
    //端口
    private static int port = 9300;

    TransportClient client;
    @Before
    public void getClient() throws UnknownHostException {
        //构建Settings, 可以设置集群名，默认是elasticsearch，如果不选择可以不构建Settings
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch")
                .build();

        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port));

        //默认集群名的话，可以不构建
//        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
    }

    @After
    public void close(){
        if(client != null){
            client.close();
        }
    }

    /**
     * 创建Document
     */
    @Test
    public void createDocument(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "jack");
        jsonObject.addProperty("age", 35);
        jsonObject.addProperty("title", "developer");
        jsonObject.addProperty("salary", 50000);
        jsonObject.addProperty("join_date", "2010-10-10");

        IndexResponse indexResponse = client.prepareIndex("company", "employee", "4")
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
    public void getDocument(){
        GetResponse response = client.prepareGet("company", "employee", "1").get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 修改Document
     */
    @Test
    public void updateDocument(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "roy");
        jsonObject.addProperty("age", 35);
        jsonObject.addProperty("title", "developer");
        jsonObject.addProperty("salary", 60000);
        jsonObject.addProperty("join_date", "2010-10-10");

        UpdateResponse indexResponse = client.prepareUpdate("company", "employee", "1")
                .setDoc(jsonObject.toString(), XContentType.JSON).get();

        System.out.println("Index Name:" + indexResponse.getIndex());
        System.out.println("Type:" + indexResponse.getType());
        System.out.println("文档ID：" + indexResponse.getId());
        System.out.println("当前实例的状态：" + indexResponse.status());

        System.out.println(indexResponse.toString());
    }

    /**
     * 删除Document
     */
    @Test
    public void deleteDocument(){
        DeleteResponse indexResponse = client.prepareDelete("company", "employee", "1").get();

        System.out.println("Index Name:" + indexResponse.getIndex());
        System.out.println("Type:" + indexResponse.getType());
        System.out.println("文档ID：" + indexResponse.getId());
        System.out.println("当前实例的状态：" + indexResponse.status());

        System.out.println(indexResponse.toString());
    }

    /**
     * 查询Document
     */
    @Test
    public void searchDocument(){
        SearchResponse response = client.prepareSearch("company") //查询的index，可以查询多个
                .setTypes("employee") //查询的type，可以查询多个
                .setQuery(QueryBuilders.matchQuery("title", "technique"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(30).to(40))
                .setFrom(0).setSize(1)
                .get();

        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("Index:" + hit.getIndex());
            System.out.println("Type:" + hit.getType());
            System.out.println("ID:" + hit.getId());
            System.out.println("Score:" + hit.getScore());
            System.out.println("Source:" + hit.getSource());
        }
    }

    /**
     * 准备数据
     */
    @Test
    public void prepareDocument(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("name", "jack");
        jsonObject.addProperty("age", 35);
        jsonObject.addProperty("title", "technique");
        jsonObject.addProperty("salary", 50000);
        jsonObject.addProperty("join_date", "2011-10-10");

        IndexResponse indexResponse = client.prepareIndex("company", "employee", "1")
                .setSource(jsonObject.toString(), XContentType.JSON).get();


        jsonObject.addProperty("name", "roy");
        jsonObject.addProperty("age", 35);
        jsonObject.addProperty("title", "boss");
        jsonObject.addProperty("salary", 30000);
        jsonObject.addProperty("join_date", "2012-10-10");

        indexResponse = client.prepareIndex("company", "employee", "2")
                .setSource(jsonObject.toString(), XContentType.JSON).get();

        jsonObject.addProperty("name", "tom");
        jsonObject.addProperty("age", 35);
        jsonObject.addProperty("title", "technique");
        jsonObject.addProperty("salary", 20000);
        jsonObject.addProperty("join_date", "2013-10-10");

        indexResponse = client.prepareIndex("company", "employee", "3")
                .setSource(jsonObject.toString(), XContentType.JSON).get();

        jsonObject.addProperty("name", "jazz");
        jsonObject.addProperty("age", 25);
        jsonObject.addProperty("title", "test");
        jsonObject.addProperty("salary", 10000);
        jsonObject.addProperty("join_date", "2014-10-10");

        indexResponse = client.prepareIndex("company", "employee", "4")
                .setSource(jsonObject.toString(), XContentType.JSON).get();
    }

    /**
     * 聚合分析
     */
    @Test
    public void aggregateDocument(){
        SearchResponse response = client.prepareSearch("company")
                .setTypes("employee")
                .addAggregation(AggregationBuilders.terms("group_by_title").field("title")
                        .subAggregation(AggregationBuilders.dateHistogram("group_by_join_date")
                                .field("join_date")
                                .dateHistogramInterval(DateHistogramInterval.YEAR)
                                .subAggregation(AggregationBuilders.avg("avg_salary").field("salary"))))
                .execute().actionGet();

        Map<String, Aggregation> aggrMap = response.getAggregations().asMap();
        StringTerms groupByTitle = (StringTerms) aggrMap.get("group_by_title");
        Iterator<StringTerms.Bucket> groupByTitleBucketIterator = groupByTitle.getBuckets().iterator();

        while(groupByTitleBucketIterator.hasNext()) {
            StringTerms.Bucket groupByTitleBucket = groupByTitleBucketIterator.next();

            if(groupByTitleBucket.getDocCount() > 0) {
                System.out.println(groupByTitleBucket.getKey() + "\t" + groupByTitleBucket.getDocCount());

                Histogram groupByJoinDate = (Histogram) groupByTitleBucket.getAggregations().asMap().get("group_by_join_date");
                Iterator<org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket> groupByJoinDateBucketIterator = (Iterator<Histogram.Bucket>) groupByJoinDate.getBuckets().iterator();

                while (groupByJoinDateBucketIterator.hasNext()) {
                    org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket groupByJoinDateBucket = groupByJoinDateBucketIterator.next();

                    if(groupByJoinDateBucket.getDocCount() > 0) {
                        System.out.println(groupByJoinDateBucket.getKey() + "\t" + groupByJoinDateBucket.getDocCount());

                        Avg avgSalary = (Avg) groupByJoinDateBucket.getAggregations().asMap().get("avg_salary");
                        System.out.println(avgSalary.getValue());
                    }
                }
            }
        }
    }
}
