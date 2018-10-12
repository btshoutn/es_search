package com.elastic.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.elastic.common.conn.EsClient;
import com.elastic.service.inter.SuperMarketSearchService;
import com.elastic.service.pojo.*;
import com.elastic.service.vo.*;
import com.elastic.util.Constants;
import com.elastic.util.SpringApplicationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.*;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.BiConsumer;

import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;

/**
 * Created by xiaotian on 2017/12/23.
 */
@Service
public class SuperMarketSearchServiceImpl implements SuperMarketSearchService {

    private EsClient esClient;

    @Override
    public String searchByBean(SearchBean bean) {
        try {
            esClient = SpringApplicationUtils.getBean(EsClient.class);
            SearchRequestBuilder searchRequestBuilder = esClient.getConnection().prepareSearch("marketsuper").setTypes("goods");
            long s = System.currentTimeMillis();
            SearchReturn returns = new SearchReturn();
            bean.setIsFacet(bean.getIsFacet() == null ? true : bean.getIsFacet());
            if (bean.getIsFacet()) {
                //聚合
                String[] attrFacet = new String[]{"attr_name"};
                bean.setFacetFields(attrFacet);

            }
            SearchResponse searchResponse = assemQueryByBean(searchRequestBuilder, esClient, bean);

            bean.setFacetFields(null);
            SearchResponse response = null;
            SearchRequestBuilder requestBuilder = esClient.getConnection().prepareSearch("marketsuper").setTypes("goods");
            if (bean.getIsFacet()) {
                String[] docAttrFields = getDocAttrFields(searchResponse);
                bean.setFacetFields(docAttrFields);

                response = assemQueryByBean(requestBuilder, esClient, bean);
                List<SearchData> searchDatas = assemSearchDatas(bean, response);
                returns.setSearchData(searchDatas);
                List<ProductInfo> productInfos = assemProductInfo(response);
                returns.setProList(productInfos);
            } else {
                response = assemQueryByBean(requestBuilder, esClient, bean);
                List<SearchData> searchDatas = assemSearchDatas(bean, response);
                returns.setSearchData(searchDatas);
                List<ProductInfo> productInfos = assemProductInfo(response);
                returns.setProList(productInfos);
            }

            returns.setNumFound(response != null ? response.getHits().getTotalHits() : 0);
            int start = bean.getStart() != null ? bean.getStart() : 0;
            int size = bean.getSize() == null ? Constants.DEFAULT_SIZE : bean.getSize();
            returns.setPage(start / size + 1);
            returns.setMaxPage(response != null && response.getHits().getTotalHits() % size == 0 ? response.getHits().getTotalHits() / size : response.getHits().getTotalHits() / size + 1);
            ResultRetrun<SearchReturn> returnResult = new ResultRetrun<>();
            returnResult.setData(returns);
            Result re = new Result();
            re.setCode(200);
            re.setMessage("success");
            returnResult.setResult(re);
            return JSON.toJSONString(returnResult);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }

        return null;
    }

    private List<ProductInfo> assemProductInfo(SearchResponse response) {
        List<ProductInfo> prolist = new ArrayList<>();
        SearchHit[] hits = response.getHits().getHits();
        if (hits != null && hits.length > 0) {
            for (SearchHit hit : hits) {
                try {
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    ProductInfo pro = new ProductInfo();
                    pro.setSku(sourceAsMap.getOrDefault(Constants.SKU, "").toString());
                    pro.setSpu(sourceAsMap.getOrDefault(Constants.SPU, "").toString());
                    pro.setGomeSku(sourceAsMap.getOrDefault(Constants.GOME_SKU, "").toString());
                    pro.setIsStock((boolean) sourceAsMap.getOrDefault(Constants.IS_IN_STORE, false));
                    pro.setIsSelf((boolean) sourceAsMap.getOrDefault(Constants.IS_SELF, false));
                    pro.setShopFlag(Integer.parseInt(sourceAsMap.getOrDefault(Constants.SHOP_FLAG, -1).toString()));
                    pro.setProTitle(sourceAsMap.getOrDefault(Constants.PRODUCT_CH, "").toString());
                    pro.setAdver(sourceAsMap.getOrDefault(Constants.ADVER, "").toString());
                    pro.setCurrentPrice(String.format("%.2f", new BigDecimal(sourceAsMap.getOrDefault(Constants.PRODUCT_SALE_PRICE, 0).toString()).doubleValue()));
                    pro.setCommentRate(sourceAsMap.getOrDefault(Constants.GOOD_COMMENT_RATE, "").toString());
                    pro.setProImgUrl(sourceAsMap.getOrDefault(Constants.PRODUCT_IMG, "").toString());
                    // 是否默认
                    pro.setIsSku((boolean) sourceAsMap.getOrDefault(Constants.IS_SKU, false));
                    prolist.add(pro);
                } catch (Exception e) {
                    e.printStackTrace();

                }
            }
        }
        return prolist;
    }

    private List<SearchData> assemSearchDatas(SearchBean bean, SearchResponse response) {

        List<SearchData> searchdatas = new ArrayList<>();

        Aggregations aggregations = response.getAggregations();
        Map<String, Aggregation> asMap = aggregations.getAsMap();
        List<String> list = new ArrayList<>();
        for (String key : bean.getFacetFields()) {
            InternalFilter filter = aggregations.get(key);
            Map<String, Aggregation> aggMap = filter.getAggregations().getAsMap();
            Iterator<String> iterator = filter.getAggregations().getAsMap().keySet().iterator();
            while (iterator.hasNext()) {
                String keys = iterator.next();
                if (aggMap.get(keys) == null || aggMap.get(keys) instanceof UnmappedTerms) {
                    continue;
                }
                StringTerms aggregation = (StringTerms) aggMap.get(keys);

                if (Constants.CATEGORY_THIRD.equals(keys)) {
                    SearchData data = spellItems(keys, aggregation.getBuckets(), Constants.KeyEnum.CATEGORYEN, true);
                    data.setSort(1);
                    searchdatas.add(data);

                } else if (Constants.CATEGORY_THIRD_NAME.equals(keys)) {
                    SearchData data = spellItems(keys, aggregation.getBuckets(), Constants.KeyEnum.CATEGORY_THIRD, true);
                    data.setSort(2);
                    searchdatas.add(data);
                } else if (keys.endsWith(Constants._ATTR)) {
                    //属性
                    SearchData data = spellAttr(keys, aggregation.getBuckets());
                    data.setSort(3);
                    searchdatas.add(data);
                }


            }
        }

        Range range = response.getAggregations().get("range");
        SearchData spellPrice = spellPrice(range.getBuckets(), Constants.PRODUCT_PROTO_PRICE);
        spellPrice.setSort(4);
        searchdatas.add(spellPrice);
        searchdatas.sort((pre, after) -> pre.getSort() - after.getSort());
        return searchdatas;
    }

    /**
     * 封装数据
     *
     * @param keys    字段名
     * @param buckets 字段值列表
     * @param keyEnum 表示名称
     * @param isShow  是否显示
     * @return
     */
    private SearchData spellItems(String keys, List<StringTerms.Bucket> buckets, Constants.KeyEnum keyEnum, boolean isShow) {
        SearchData data = new SearchData();
        List<Item> items = spellItem(buckets);
        data.setDescpName(keyEnum.getValue());
        data.setField(keys);
        data.setIsShow(isShow);
        // data.setSort(1);
        data.setCount(buckets.size());
        data.setItemArray(items);
        return data;
    }

    /**
     * 封装数据
     *
     * @param buckets 字段值列表
     * @return
     */
    private SearchData spellPrice(List<? extends Range.Bucket> buckets, String priceField) {
        SearchData data = new SearchData();
        List<Item> items = new ArrayList<>();
        buckets.forEach(bucket -> {
            Item item = new Item();
            item.setId(bucket.getKeyAsString());
            item.setText(bucket.getKeyAsString());
            item.setCount(bucket.getDocCount());
            items.add(item);
        });
        data.setDescpName(Constants.PRICE_CH);
        data.setField(priceField);
        data.setIsShow(true);
        // data.setSort(1);
        data.setCount(buckets.size());
        data.setItemArray(items);
        return data;
    }

    /**
     * 封装数据
     *
     * @param keys    字段名
     * @param buckets 字段值列表
     * @return
     */
    private SearchData spellAttr(String keys, List<StringTerms.Bucket> buckets) {
        SearchData data = new SearchData();
        List<Item> items = spellItem(buckets);
        data.setDescpName(keys.split("\\.")[1].split("_")[0]);
        data.setField(keys);
        data.setIsShow(true);
        // data.setSort(1);
        data.setCount(buckets.size());
        data.setItemArray(items);
        return data;
    }

    private List<Item> spellItem(List<StringTerms.Bucket> buckets) {
        List<Item> items = new ArrayList<>();
        buckets.forEach(bucket -> {
            Item item = new Item();
            item.setId(bucket.getKeyAsString());
            item.setCount(bucket.getDocCount());
            item.setText(bucket.getKeyAsString());
            items.add(item);
        });
        return items;
    }


    private String[] getDocAttrFields(SearchResponse searchResponse) {
        List<String> attrFieldList = new ArrayList<>();
        attrFieldList.add("category_third");
        attrFieldList.add("category_third_id");
        if (searchResponse.getAggregations().get("attr_name") != null) {
            InternalFilter aggFilter = searchResponse.getAggregations().get("attr_name");
            Map<String, Aggregation> aggMap = aggFilter.getAggregations().getAsMap();
            Iterator<String> iterator = aggFilter.getAggregations().getAsMap().keySet().iterator();
            while (iterator.hasNext()) {
                String keys = iterator.next();
                StringTerms aggregation = (StringTerms) aggMap.get(keys);
                aggregation.getBuckets().forEach(bucket -> {
                    attrFieldList.add("dynamicFields." + bucket.getKey() + "");
                    System.out.println(keys + "-->" + bucket.getKey() + ":" + bucket.getDocCount());
                });

            }
        }

        return attrFieldList.toArray(new String[attrFieldList.size()]);
    }

    @Override
    public String suggestByBean(SearchBean bean) {
        return null;
    }


   /* private SearchResponse minMaxQuery(ScoreMode scoreMode, int minChildren, Integer maxChildren) throws SearchPhaseExecutionException {
        HasChildQueryBuilder hasChildQuery = hasChildQuery(
                "child",
                QueryBuilders.functionScoreQuery(constantScoreQuery(QueryBuilders.termQuery("foo", "two")),
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(weightFactorFunction(1)),
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery("foo", "three"), weightFactorFunction(1)),
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(QueryBuilders.termQuery("foo", "four"), weightFactorFunction(1))
                        }).boostMode(CombineFunction.REPLACE).scoreMode(FiltersFunctionScoreQuery.ScoreMode.SUM), scoreMode)
                .minMaxChildren(minChildren, maxChildren != null ? maxChildren : HasChildQueryBuilder.DEFAULT_MAX_CHILDREN);

        return client()
                .prepareSearch("test")
                .setQuery(hasChildQuery)
                .addSort("_score", SortOrder.DESC).addSort("id", SortOrder.ASC).get();
    }*/

    private SearchResponse assemQueryByBean(SearchRequestBuilder searchRequest, EsClient esClient, SearchBean bean) throws Exception {


        Map<String, Object> params = new HashMap<>();
        params.put("num1", 1);
        params.put("num2", 10);

        //https://blog.csdn.net/ctwy291314/article/details/82222076
        // score_mode计算functions中的分数形式，加减乘除，boost_mode计算最外层的分数形式，加减乘除。所以最后总分是tf/idf分数加上脚本得分
        //String inlineScript = "long age;if (doc['age'].value < 45)  age = doc['age'].value + 50; return age * params.num1;";
        String inlineScript = " return ( doc['spu_score'].value + 1) * params.num2;";
        // + "return (diff +num1+num2)";
        //Script script = new Script(ScriptType.INLINE, "painless", inlineScript, params);
        Script script = new Script(ScriptType.STORED, "painless", inlineScript, params);
        //Script script = new Script(ScriptType.readFrom(new InputStreamStreamInput(new FileInputStream(new File("")))), "painless", inlineScript, params);
        ScriptScoreFunctionBuilder scriptScoreFunctionBuilder = ScoreFunctionBuilders.scriptFunction(script);
        //searchRequest.setQuery(functionScoreQuery(QueryBuilders.matchQuery("name","中华").operator(Operator.AND),scriptScoreFunctionBuilder));
        //  打分参考  https://www.programcreek.com/java-api-examples/index.php?api=org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder
        searchRequest.setQuery(functionScoreQuery(bean.getQuery() != null ? QueryBuilders.matchQuery("full_name", bean.getQuery()) : QueryBuilders.matchAllQuery(), scriptScoreFunctionBuilder).boostMode(CombineFunction.REPLACE).scoreMode(FunctionScoreQuery.ScoreMode.SUM));
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (bean.getFilter() != null) {
            Filter fq = bean.getFilter();

            if (fq.getCatFirst() != null && fq.getCatFirst().length > 0) {
                boolQueryBuilder.must(this.getFilterQuery(Constants.CATEGORY_FIRST, fq.getCatFirst(), Constants.OR));
            }

            if (fq.getCatSecond() != null && fq.getCatSecond().length > 0) {
                boolQueryBuilder.must(this.getFilterQuery(Constants.CATEGORY_SECOND, fq.getCatSecond(), Constants.OR));
            }

            if (fq.getCatThird() != null && fq.getCatThird().length > 0) {
                //条件
                boolQueryBuilder.must(this.getFilterQuery(Constants.CATEGORY_THIRD, fq.getCatThird(), "OR"));
            }
            if (fq.getFgCatFirst() != null && fq.getFgCatFirst().length > 0) {
                boolQueryBuilder.must(getFilterQuery(Constants.FG_CATEGORY_FIRST, fq.getFgCatFirst(), Constants.OR));
            }
            if (fq.getFgCatSecond() != null && fq.getFgCatSecond().length > 0) {
                boolQueryBuilder.must(getFilterQuery(Constants.FG_CATEGORY_SECOND, fq.getFgCatSecond(), Constants.OR));
            }
            if (fq.getFgCatThird() != null && fq.getFgCatThird().length > 0) {
                boolQueryBuilder.must(getFilterQuery(Constants.FG_CATEGORY_THIRD, fq.getFgCatThird(), Constants.OR));
            }

            if (fq.getCountry() != null && fq.getCountry().length > 0) {
                boolQueryBuilder.must(getFilterQuery(Constants.COUNTRY_ID, fq.getCountry(), Constants.OR));
            }
            if (fq.getPromotType() != null && fq.getPromotType().length > 0) {
                boolQueryBuilder.must(getFilterQuery(Constants.PROMOT_TYPE, fq.getPromotType(), Constants.OR));
            }
            if (fq.getIsInStore() != null) {
                boolQueryBuilder.must(getFilterQuery(Constants.IS_IN_STORE, new Integer[]{fq.getIsInStore()}, Constants.OR));
            }
            if (fq.getIsSelf() != null) {
                boolQueryBuilder.must(getFilterQuery(Constants.IS_SELF, new Integer[]{fq.getIsInStore()}, Constants.OR));
            }
            //页面选择的过滤属性
            if (fq.getAttr() != null && fq.getAttr().length > 0) {
                BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
                for (Attrbute attr : fq.getAttr()) {
                    getEscapeFilterQuery(queryBuilder, attr.getAttrbuteId(), attr.getAttrbuteValues(), Constants.OR);
                }
                boolQueryBuilder.must(queryBuilder);
            }

        }


        if (bean.getFacetFields() != null) {

            for (String field : bean.getFacetFields()) {
                TermsAggregationBuilder aggFieldBuilder = AggregationBuilders.terms(field).field(field);
                aggFieldBuilder.size(1000);
                AggregationBuilder filter = AggregationBuilders.filter(field, boolQueryBuilder);
                filter.subAggregation(aggFieldBuilder);
                searchRequest.addAggregation(filter);
            }
        }


        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders.range("range").field("product_sale_price").addRange(0, 30).addRange(30, 50).addRange(50, 100);
        searchRequest.addAggregation(rangeAggregationBuilder);
        searchRequest.setPostFilter(boolQueryBuilder);

        Integer size = bean.getSize() == null ? Constants.DEFAULT_SIZE : bean.getSize();
        searchRequest.setSize(size);
        if (bean.getPage() != null && bean.getPage() > 0) {
            searchRequest.setFrom((bean.getPage() - 1) * size);
        } else {
            searchRequest.setFrom(0);
        }
        if (bean.getSortField() != null) {
            searchRequest.addSort(bean.getSortField(), bean.getSortType() == 0 ? SortOrder.ASC : SortOrder.DESC);
        }


        searchRequest.setExplain(true);

        System.out.println("param:" + searchRequest);
        SearchResponse searchResponse = searchRequest.get();
        return searchResponse;

    }


    private void getEscapeFilterQuery(BoolQueryBuilder queryBuilder, String fieldName, Object[] fieldValues, String andor) {
        for (int i = 0; i < fieldValues.length; i++) {
            if ("OR".equals(andor)) {
                queryBuilder.should(QueryBuilders.termQuery(fieldName, fieldValues[i]));
            } else if ("AND".equals(andor)) {
                queryBuilder.must(QueryBuilders.termQuery(fieldName, fieldValues[i]));
            }
        }
    }


    private BoolQueryBuilder getFilterQuery(String fieldName, Object[] fieldValues, String andor) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        for (int i = 0; i < fieldValues.length; i++) {
            if ("OR".equals(andor)) {
                queryBuilder.should(QueryBuilders.matchQuery(fieldName, fieldValues[i]).operator(Operator.OR));
            } else if ("AND".equals(andor)) {
                queryBuilder.must(QueryBuilders.matchQuery(fieldName, fieldValues[i]).operator(Operator.AND));
            }
        }

        return queryBuilder;

    }

    private QueryBuilder getFilterQuery(String fieldName, Object[] fieldValues, String andor, BoolQueryBuilder queryBuilder) {
        //BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (int i = 0; i < fieldValues.length; i++) {
            if ("OR".equals(andor)) {
                queryBuilder.should(QueryBuilders.matchQuery(fieldName, fieldValues[i]).operator(Operator.OR));
            } else if ("AND".equals(andor)) {
                queryBuilder.must(QueryBuilders.matchQuery(fieldName, fieldValues[i]).operator(Operator.AND));
            }
        }

        return queryBuilder;

    }

    private void assemQueryParam(SearchBean bean, SearchReturn returns) {
        SearchParams params = new SearchParams();
        params.setKeyword(bean.getQuery());
        Filter f = bean.getFilter();
        if (f != null) {
            String[] catFirst = f.getCatFirst();
            String[] catSecond = f.getCatSecond();
            String[] catThird = f.getCatThird();
            Integer[] brand = f.getBrand();
            Integer[] country = f.getCountry();
            String[] fgCatFirst = f.getFgCatFirst();
            String[] fgCatSecond = f.getFgCatSecond();
            String[] fgCatThird = f.getFgCatThird();
            String[] whiteShopIds = f.getWhiteShopIds();
            String[] whiteCateIds = f.getWhiteCateIds();
            String[] whiteBrandIds = f.getWhiteBrandIds();
            String[] whiteProdIds = f.getWhiteProdIds();
            String[] blackShopIds = f.getBlackShopIds();
            String[] blackCateIds = f.getBlackCateIds();
            String[] blackBrandIds = f.getBlackBrandIds();
            String[] blackProdIds = f.getBlackProdIds();
            String[] activityIds = f.getActivityIds();

            if (catFirst != null && catFirst.length > 0) {
                params.setCatFirst(catFirst);
            }
            if (catSecond != null && catSecond.length > 0) {
                params.setCatSecond(catSecond);
            }
            if (catThird != null && catThird.length > 0) {
                params.setCatThird(catThird);
            }
            if (fgCatFirst != null && fgCatFirst.length > 0) {
                params.setFgCatFirst(fgCatFirst);
            }
            if (fgCatSecond != null && fgCatSecond.length > 0) {
                params.setFgCatSecond(fgCatSecond);
            }
            if (fgCatThird != null && fgCatThird.length > 0) {
                params.setFgCatThird(fgCatThird);
            }
            if (brand != null && brand.length > 0) {
                params.setBrand(brand);
            }
            if (country != null && country.length > 0) {
                params.setCountry(country);
            }
            if (activityIds != null && activityIds.length > 0) {
                params.setActivityIds(activityIds);
            }
        }
        returns.setSearchParams(params);
    }
}
