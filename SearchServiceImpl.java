package com.elastic.service.impl;

import com.elastic.common.conn.EsClient;
import com.elastic.service.inter.SearchService;
import org.apache.lucene.index.Term;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.*;
import org.elasticsearch.join.aggregations.Children;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;

/**
 * Created by xiaotian on 2017/12/2.
 */
@Service
public class SearchServiceImpl implements SearchService {
    @Autowired
    private EsClient client;

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SearchServiceImpl.class);

    public void search() {
        SearchResponse searchResponse = client.getConnection().prepareSearch("twitter")
                .setTypes("tweet")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("name","三胖子"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(19).to(400))
                .setFrom(0).setSize(20).setExplain(true)
                //.addAggregation(AggregationBuilder.CommonFields.FIELD.match(""))
                .get();

        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsMap());
        }
    }

    public void searchByCondition()  throws Exception{


        SearchRequestBuilder searchRequestBuilder = client.getConnection().prepareSearch("red89")
                .setTypes("test");


        Map<String, Object> params = new HashMap<>();
        params.put("num1", 1);
        params.put("num2", 2);

        //String inlineScript = "long age;if (doc['age'].value < 45)  age = doc['age'].value + 50; return age * params.num1;";
        String inlineScript = " return doc['age'].value * params.num1;";
               // + "return (diff +num1+num2)";
        Script script = new Script(ScriptType.INLINE,"painless",inlineScript , params);
        ScriptScoreFunctionBuilder scriptScoreFunctionBuilder = ScoreFunctionBuilders.scriptFunction(script);



        //MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "中华");

        searchRequestBuilder.setQuery(functionScoreQuery(QueryBuilders.matchQuery("name","中华").operator(Operator.AND),scriptScoreFunctionBuilder));
       // searchRequestBuilder.setQuery(QueryBuilders.matchQuery("name","中华").operator(Operator.AND));
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        getFilterQuery("tel",new String[]{"18612855433"},"AND",queryBuilder);
       // getFilterQuery("name",new String[]{"中华人民共和国"},"AND",queryBuilder);


        //getFilterQuery("age",new String[]{"40"},"AND",queryBuilder);
        getFilterQuery("message",new String[]{"程序设计19"},"AND",queryBuilder);
        boolQueryBuilder.must(queryBuilder);
        //boolQueryBuilder.must();
       // boolQueryBuilder.must(getRangeFilterQuery("age",new Integer[]{null,45}));
        //new_score = old_score * log(1 + factor * number_of_votes)

       FieldValueFactorFunctionBuilder age = ScoreFunctionBuilders.fieldValueFactorFunction("age").modifier(FieldValueFactorFunction.Modifier.LN1P).factor(10);
        FieldValueFactorFunctionBuilder age1 = ScoreFunctionBuilders.fieldValueFactorFunction("age").modifier(FieldValueFactorFunction.Modifier.LN1P).factor(4);
        FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders =
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{new FunctionScoreQueryBuilder.FilterFunctionBuilder(age),new FunctionScoreQueryBuilder.FilterFunctionBuilder(age1)};
        //FunctionScoreQueryBuilder scoreQueryBuilder = functionScoreQuery(boolQueryBuilder, filterFunctionBuilders).boostMode(CombineFunction.SUM);

       // FunctionScoreQueryBuilder functionScoreQueryBuilder = functionScoreQuery(boolQueryBuilder, scriptScoreFunctionBuilder);
        searchRequestBuilder.setPostFilter(boolQueryBuilder);
        searchRequestBuilder.setFrom(0);
        searchRequestBuilder.setSize(20);
        searchRequestBuilder.setExplain(true);

        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("agg").field("attr_name");//.subAggregation(AggregationBuilders.topHits("top").from(0).size(10)).size(100);
        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders.range("range").field("age").addRange(0, 30).addRange(30,50).addRange(50,100);

        aggregationBuilder.size(100);

        AggregationBuilder filter = AggregationBuilders.filter("agg", boolQueryBuilder)
                .subAggregation(aggregationBuilder);

        searchRequestBuilder.addAggregation(filter);
        searchRequestBuilder.addAggregation(rangeAggregationBuilder);
       // searchRequestBuilder.addAggregation(filter);
        //searchRequestBuilder.addSort("age",SortOrder.DESC);





        //打分
        //searchRequestBuilder.addRescorer(RescoreBuilder.queryRescorer(QueryBuilders.functionScoreQuery(ScoreFunctionBuilders.fieldValueFactorFunction("age"))));

        SearchResponse searchResponse1 = searchRequestBuilder.get();


        System.out.println("param:"+searchRequestBuilder.toString());



        System.out.println("aaaa"+searchResponse1.getAggregations().getAsMap());


        InternalFilter aggFilter = searchResponse1.getAggregations().get("agg");
        System.out.println(aggFilter.toString());
        Terms agg = aggFilter.getAggregations().get("agg");

        agg.getBuckets().forEach(bucket ->{
            System.out.println(bucket.getKey()+":"+bucket.getDocCount());
        } );
        System.out.println("--------------------------------");
        Range range = searchResponse1.getAggregations().get("range");

        range.getBuckets().forEach(bucket ->{
            //System.out.println(bucket.getKey()+":"+bucket.getDocCount());
            //logger.info("key [{}], from [{}], to [{}], doc_count [{}]", bucket.getKey(), bucket.getFrom(),bucket.getTo(),bucket.getDocCount());
            System.out.println(String.format("key [%s], from [%s], to [%s], doc_count [%d]", bucket.getKey(), bucket.getFrom(),bucket.getTo(),bucket.getDocCount()));

    });

        SearchHits hits = searchResponse1.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsMap());
        }

        System.out.println("---------------------");

//
//        SearchRequestBuilder searchRequestBuilder1 = client.getConnection().prepareSearch("red88")
//                .setTypes("test");
////        QueryBuilder tel = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("tel","18612855433")).should(QueryBuilders.matchQuery("message","程序设计"));
////        BoolQueryBuilder filter1 = QueryBuilders.boolQuery()//.must(QueryBuilders.matchQuery("age", "40").operator(Operator.AND))
////                .must(QueryBuilders.rangeQuery("age").from(0).to(40))
////                .must(tel);
//        searchRequestBuilder.setQuery(QueryBuilders.matchQuery("name","三胖子").operator(Operator.AND));
//        BoolQueryBuilder boolQueryBuilder1 = QueryBuilders.boolQuery();
//        BoolQueryBuilder queryBuilder1 = QueryBuilders.boolQuery();
//        getFilterQuery("tel",new String[]{"18612855433"},"OR",queryBuilder1);
//        getFilterQuery("message",new String[]{"程序设计"},"OR",queryBuilder1);
//        boolQueryBuilder.must(queryBuilder);
//        //boolQueryBuilder.must();
//        boolQueryBuilder.must(getRangeFilterQuery("age",new Integer[]{null,40}));
//        searchRequestBuilder.setPostFilter(boolQueryBuilder1);
//        searchRequestBuilder.setFrom(0);
//        searchRequestBuilder.setSize(20);
//        searchRequestBuilder.setExplain(true);
//        TermsAggregationBuilder aggregationBuilder1 = AggregationBuilders.terms("agg").field("attr_name");
//
//        aggregationBuilder.size(100);
//        searchRequestBuilder.addAggregation(aggregationBuilder);
//        searchRequestBuilder.addSort("age",SortOrder.DESC);
//




    }

    public FunctionScoreQueryBuilder sortByFucntion(QueryBuilder queryBuilder) {
        FunctionScoreQueryBuilder query = functionScoreQuery(queryBuilder,
                ScoreFunctionBuilders.fieldValueFactorFunction("age").modifier(FieldValueFactorFunction.Modifier.LN1P).factor(1f)).boostMode(CombineFunction.SUM);
        //.add(ScoreFunctionBuilders.fieldValueFactorFunction(查询字段).modifier(Modifier.RECIPROCAL).factor(1)).boostMode(“sum”);
        return query;
    }
    private QueryBuilder getFilterQuery(String fieldName, Object[] fieldValues,String andor,BoolQueryBuilder queryBuilder) {
        //BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (int i = 0; i < fieldValues.length; i++) {
            if ("OR".equals(andor)){
                queryBuilder.should(QueryBuilders.matchQuery(fieldName,fieldValues[i]).operator(Operator.OR));
            }else if ("AND".equals(andor)){
                queryBuilder.must(QueryBuilders.matchQuery(fieldName,fieldValues[i]).operator(Operator.AND));
            }
        }

        return  queryBuilder;

    }
    private QueryBuilder getFilterQuery(String fieldName, Object[] fieldValues,String andor) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (int i = 0; i < fieldValues.length; i++) {
            if ("OR".equals(andor)){
                boolQueryBuilder.should(QueryBuilders.matchQuery(fieldName,fieldValues[i]));
            }else if ("AND".equals(andor)){
             boolQueryBuilder.must(QueryBuilders.matchQuery(fieldName,fieldValues[i]));
            }
        }

        return  boolQueryBuilder;

    }

    private RangeQueryBuilder getRangeFilterQuery(String fieldName, Integer[] values) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(fieldName);
        rangeQueryBuilder.from(values[0]==null?0:values[0]);
        rangeQueryBuilder.to((values.length<2||values[1]==null)?99999999:values[1]);
        return rangeQueryBuilder;
    }


    public void multiSearch() {

        SearchRequestBuilder srb1 = client.getConnection().prepareSearch().setQuery(QueryBuilders.matchQuery("name", "tom")).setSize(1);
        SearchRequestBuilder srb2 = client.getConnection().prepareSearch().setQuery(QueryBuilders.matchQuery("name", "三胖子")).setSize(1);
        MultiSearchResponse multiSearchResponse = client.getConnection().prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .get();

        for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
            for (SearchHit hit : item.getResponse().getHits().getHits()) {
                System.out.println(hit.getSourceAsMap());

            }
        }

    }

    public void aggsearch() {
        SearchRequestBuilder requestBuilder = client.getConnection().prepareSearch("twitter").setTypes("tweet")
                .setFrom(0).setSize(100);
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("agg").field("name").subAggregation(AggregationBuilders.terms("add").field("age"));
        SearchResponse response = requestBuilder.setQuery(QueryBuilders.matchQuery("name", "三胖"))
                .addAggregation(aggregationBuilder)
                .addSort("age", SortOrder.DESC)
                .setExplain(true).execute().actionGet();
        SearchHits searchHits = response.getHits();

        Terms agg = response.getAggregations().get("agg");
        Children children = response.getAggregations().get("agg");
        System.out.println(agg.getBuckets());
        for (Terms.Bucket bucket : agg.getBuckets()) {
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());
        }
        System.out.println(children.getAggregations().getAsMap());

    }

    public void metricsAgg() {

        SearchRequestBuilder searchRequestBuilder = client.getConnection().prepareSearch("hello").setTypes("test").setFrom(0).setSize(100);
        MinAggregationBuilder minAggregationBuilder = AggregationBuilders.min("agg").field("age");
        SearchResponse response = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(minAggregationBuilder).setExplain(true).execute().actionGet();
        Min min = response.getAggregations().get("agg");
        System.out.println("min:"+min.getValue());
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("max_age").field("age");
        SearchResponse response1 = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(maxAggregationBuilder).setExplain(true).execute().actionGet();
        Max max_age = response1.getAggregations().get("max_age");
        System.out.println("max:"+max_age.getValue());

        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("avg_age").field("age");
        SearchResponse response2 = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(avgAggregationBuilder).setExplain(true).execute().actionGet();
        Avg avg_age = response2.getAggregations().get("avg_age");
        System.out.println("avg_age:"+avg_age.getValue());

        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("sum_age").field("age");
        SearchResponse searchResponse = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery()).addAggregation(sumAggregationBuilder)
                .setExplain(true).execute().actionGet();
        Sum sum_age = searchResponse.getAggregations().get("sum_age");
        System.out.println("sum_age:"+sum_age.getValue());

        System.out.println("---------------------------");

        StatsAggregationBuilder statsAggregationBuilder = AggregationBuilders.stats("agg_status").field("age");
        SearchResponse searchResponse1 = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery()).addAggregation(statsAggregationBuilder)
                .setExplain(true).execute().actionGet();

        Stats agg_status = searchResponse1.getAggregations().get("agg_status");
        double min1 = agg_status.getMin();
        double max = agg_status.getMax();
        long count = agg_status.getCount();
        double sum = agg_status.getSum();
        double avg = agg_status.getAvg();
        System.out.println("---------------------------------");
        System.out.println("min1="+min1);
        System.out.println("max="+max);
        System.out.println("count="+count);
        System.out.println("sum="+sum);
        System.out.println("avg="+avg);

        ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder = AggregationBuilders.extendedStats("extend_status").field("age");

        SearchResponse searchResponse2 = searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery()).addAggregation(extendedStatsAggregationBuilder).setExplain(true).execute().actionGet();
        ExtendedStats extend_status = searchResponse2.getAggregations().get("extend_status");
        double extend_min = extend_status.getMin();
        double extend_max = extend_status.getMax();
        long extend_count = extend_status.getCount();
        double extend_sum = extend_status.getSum();
        double extend_avg = extend_status.getAvg();
        System.out.println("---------------------------------");
        System.out.println("extend_min="+extend_min);
        System.out.println("extend_max="+extend_max);
        System.out.println("extend_count="+extend_count);
        System.out.println("extend_sum="+extend_sum);
        System.out.println("extend_avg="+extend_avg);


    }
}
