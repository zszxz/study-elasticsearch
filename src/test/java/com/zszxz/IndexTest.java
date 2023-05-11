package com.zszxz;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author lsc
 * <p> </p>
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class IndexTest {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Test
    public void createIndex() throws IOException {
        // 创建索引 ,请求对象
        CreateIndexRequest request = new CreateIndexRequest("course");
        // 发送请求，获取响应
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        // 响应状态
        System.out.println("是否创建成功： " + acknowledged);

        // 关闭客户端连接
        restHighLevelClient.close();

    }

    @Test
    public void viewIndex() throws IOException {
        // 查看索引请求对象
        GetIndexRequest request = new GetIndexRequest("course");
        // 发送请求取响应
        GetIndexResponse response = restHighLevelClient.indices().get(request, RequestOptions.DEFAULT);
        Map<String, MappingMetadata> mappings = response.getMappings();
        // 响应状态
        System.out.println("： " + mappings);

        // 关闭客户端连接
        restHighLevelClient.close();

    }

    @Test
    public void delIndex() throws IOException {
        // 查看索引请求对象
        DeleteIndexRequest request = new DeleteIndexRequest("course");
        // 发送请求取响应
        AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        // 响应状态
        System.out.println("是否删除成功： " + acknowledged);

        // 关闭客户端连接
        restHighLevelClient.close();

    }
}
