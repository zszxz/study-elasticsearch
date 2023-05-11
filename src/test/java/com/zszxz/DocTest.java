package com.zszxz;

import com.google.gson.Gson;
import com.zszxz.domain.Course;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.LocalDate;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lsc
 * <p> </p>
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class DocTest {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Test
    public void createDoc() throws IOException {
        // 请求对象
        IndexRequest request = new IndexRequest();
        // 设置 索引 和id
        request.index("course").id("1");
        // 参数
        Course course = new Course();
        course.setName("java");
        course.setPrice(new BigDecimal("1800"));
        course.setDescription("不贵，火爆");
        Gson gson = new Gson();
        String json = gson.toJson(course);
        // 添加文档,格式为JSON
        request.source(json, XContentType.JSON);
        // 发送请求，获取响应对象
        IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println("结果:" + response.getResult());

    }

    @Test
    public void createDocBulk() throws IOException {
        //创建请求对象
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().index("course").id("2").source(XContentType.JSON, "name", "java"));
        request.add(new IndexRequest().index("course").id("3").source(XContentType.JSON, "name", "python"));
        request.add(new IndexRequest().index("course").id("4").source(XContentType.JSON, "name", "go"));
        //发送请求，获取响应对象
        BulkResponse responses = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println("结果:" + responses.getTook());

    }
    // 更新
    @Test
    public void updateDoc() throws IOException, JSONException {
        // 请求对象
        UpdateRequest request = new UpdateRequest();
        // 设置 索引 和id
        request.index("course").id("1");
        // 参数
        // 添加文档,格式为JSON
        HashMap<String, Object> map = new HashMap<>();
        map.put("name","python");
        request.doc(map);
        // 发送请求，获取响应对象
        UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println("结果:" + response.getResult());

    }
    // 查看
    @Test
    public void viewDoc() throws IOException {
        //创建请求对象
        GetRequest request = new GetRequest().index("course").id("1");
        //发送请求，获取响应对象
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println("结果:"+ response.getSource());

    }
    // 删除
    @Test
    public void delDoc() throws IOException {
        //创建请求对象
        DeleteRequest request = new DeleteRequest().index("course").id("1");
        //发送请求，获取响应对象
        DeleteResponse response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.getResult());

    }
    // 批量删除
    @Test
    public void delDocBulk() throws IOException {
        //创建请求对象
        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest().index("course").id("2"));
        request.add(new DeleteRequest().index("course").id("3"));
        request.add(new DeleteRequest().index("course").id("4"));
        //发送请求，获取响应对象
        BulkResponse responses = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println("took:" + responses.getTook());

    }
    // 查询所有
    @Test
    public void  selectAll() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("course");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询所有数据
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }
    // 匹配查询
    @Test
    public void  selectMatch() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("course");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询匹配数据
        sourceBuilder.query(QueryBuilders.matchQuery("name","java"));
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }

    // 关键字查询
    @Test
    public void keySelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("course");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 关键字查询
        sourceBuilder.query(QueryBuilders.termQuery("name","java"));
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }

    @Test
    public void pageSelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("course");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        // 当前页
        sourceBuilder.from(1);
        // 每页显示条
        sourceBuilder.size(1);
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }
    // 排序查询
    @Test
    public void sortSelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("event");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.sort("address", SortOrder.DESC);
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }

    // 过滤查询
    @Test
    public void filterSelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("event");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        // 排除字段
        String[] excludes = {};
        // 包含字段
        String[] includes = {"name", "address"};
        sourceBuilder.fetchSource(includes, excludes);
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }

    @Test
    public void boolSelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("event");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 布尔查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        /// 必须包含
        boolQueryBuilder.must(QueryBuilders.matchQuery("name", "haixiao"));
        // 一定不包含
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("figure", "小识"));
        // 可能包含
        boolQueryBuilder.should(QueryBuilders.matchQuery("figure", "小识"));
        // 查询
        sourceBuilder.query(boolQueryBuilder);
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }

    @Test
    public void rangeSelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("event");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("date");
        // 大于等于
        rangeQuery.lte("2021-04-12 00:00:00");
        // 小于等于
        rangeQuery.lte("2021-04-13 00:00:00");
        // 查询
        sourceBuilder.query(rangeQuery);
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }

    @Test
    public void likeSelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("event");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("figure", "小*");
        // 查询
        sourceBuilder.query(queryBuilder);
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }

    @Test
    public void hightLigntSelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("event");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //高亮查询
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name","haixiao");
        //设置查询方式
        sourceBuilder.query(termsQueryBuilder);
        //构建高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //设置标签前缀
        highlightBuilder.preTags("<font color='red'>");
        //设置标签后缀
        highlightBuilder.postTags("</font>");
        //设置高亮字段
        highlightBuilder.field("name");
        //设置高亮构建对象
        sourceBuilder.highlighter(highlightBuilder);

        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            //打印高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
            System.out.println("------华丽的分割线-----");
        }
    }

    @Test
    public void aggSelect() throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("course");
        // 构建请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 聚合查询
        sourceBuilder.aggregation(AggregationBuilders.max("maxPrice").field("price"));
        // 查询
        request.source(sourceBuilder);
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        // 查询结果
        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {
            //输出每条结果信息
            System.out.println(hit.getSourceAsString());
            System.out.println("------华丽的分割线-----");
        }
    }

}
