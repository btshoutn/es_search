package com.elastic.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.elastic.common.conn.EsClient;
import com.elastic.common.constants.Constants;
import com.elastic.common.constants.EnumEntity;
import com.elastic.common.dydata.DataSourceContextHolder;
import com.elastic.common.dydata.DataSourceType;
import com.elastic.entity.GomehigoIndex;
import com.elastic.entity.po.*;
import com.elastic.mapper.*;
import com.elastic.service.inter.SuperMarketDataBaseService;
import com.elastic.util.HttpUtils;
import com.elastic.util.PropertyUtils;
import com.elastic.util.SpringApplicationUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;

/**
 * 超市索引建立
 */
@Transactional
@Service(value = "superMarketDataBaseServiceImpl")
public class SuperMarketDataBaseServiceImpl implements SuperMarketDataBaseService {

    private static final Logger logger = Logger.getLogger(SuperMarketDataBaseServiceImpl.class);

    @Resource
    private ProdSpuMapper prodSpuMapper;

    @Resource
    private ProdSkuMapper prodSkuMapper;

    @Resource
    private ProdCategoryMapper prodCategoryMapper;

    @Resource
    private CmsModuleCountryMapper cmsModuleCountryMapper;

    @Resource
    private ProdCateAttMapper prodCateAttMapper;

    @Resource
    private ProdSpuAttributeMapper prodSpuAttributeMapper;

    @Resource
    private ProdSkuAttributeMapper prodSkuAttributeMapper;

    @Resource
    private ProdAttributeValueMapper ProdAttributeValueMapper;

    @Resource
    private ProdAttributeNameMapper prodAttributeNameMapper;

    @Resource
    private CmsSolrFilterCateAttrMapper cmsSolrFilterCateAttrMapper;

    @Resource
    private CmsCategoryMapper cmsCategoryMapper;

    @Resource
    private CmsCategoryRelationMapper cmsCategoryRelationMapper;

    @Resource
    private ProdSpuCateRelationMapper prodSpuCateRelationMapper;

    private EsClient esClient;

    @Override
    public List<GomehigoIndex> getBeans(int skip, int limit) {
        logger.info("Gome SuperMarket 第" + skip + "条数据同步个数" + limit + "开始");
        List<GomehigoIndex> indexs = new ArrayList<>();
        esClient = SpringApplicationUtils.getBean(EsClient.class);
        try {
            ProdSpu spu = new ProdSpu();
            spu.setStart(skip);
            spu.setSize(limit);
            DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
            List<ProdSpu> spus = prodSpuMapper.queryByBeanForPage(spu);
            for (ProdSpu prodSpu : spus) {
                try {
                    logger.info("Gome SuperMarket spu：" + prodSpu.getSpuId() + "同步开始");
                    DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
                    List<ProdSku> prodSkus = prodSkuMapper.selectSkusBySpu(prodSpu.getSpuId());
                    int defaultIndex = 0;
                    for (int i = 0; i < prodSkus.size(); i++) {
                        ProdSku prodSku = prodSkus.get(i);
                        if (prodSku.getSkuStatus() != 4) {
                            //如果sku下架，意味着该sku不能作为主商品，因此需要将其加1
                            if (defaultIndex == i) {
                                defaultIndex = prodSkus.size() - 1 == i ? i : ++defaultIndex;
                            }
                            logger.info("Gome SuperMarket 删除下架sku：" + prodSku.getSkuId() + " spu：" + prodSpu.getSpuId());
                            esClient.getConnection().prepareDelete("gome_market","goods",prodSku.getSkuId());
                            logger.info("Gome SuperMarket 成功删除下架sku：" + prodSku.getSkuId() + " spu：" + prodSpu.getSpuId());
                            continue;
                        }
                        GomehigoIndex index = new GomehigoIndex();
                        if (i == defaultIndex) {
                            //是主商品
                            index.setIs_sku(false);
                        } else {
                            //是sku
                            index.setIs_sku(true);
                        }
                        index.setId(prodSku.getSkuId());
                        index.setSpu(prodSku.getSpuId());
                        index.setBrand_id(prodSpu.getBrandId());

                        index.setProduct_ch(prodSku.getSkuName());
                        index.setShop_id(prodSpu.getShopId()+"");
                        //assemFgCate(prodSpu, index);//前台分类
                        assemCate(prodSpu, index);//后台分类
                         //多分类
                        assemCateMultiValued(prodSpu, index);
                        index.setShop_flag(prodSpu.getIsSelf() != null && !prodSpu.getIsSelf().equals("") ? Integer.parseInt(prodSpu.getIsSelf()) : 0);
                        index.setIs_self(prodSpu.getIsSelf() != null && prodSpu.getIsSelf().equals("2"));
                        //spu分数 打分排序
                        index.setSpu_score(prodSpu.getSpuScore()==null?0:prodSpu.getSpuScore());
                        //List<String> attrNames = new ArrayList<String>();
                        //TODO 需要接口提供 规格参数
                        //Map<String, Object> attrMap = assemAttrMap(prodSku, attrNames);
                        //搜索聚合项目 attr_name
                        //index.setAttrName(attrNames.isEmpty() ? null : attrNames.toArray(new String[attrNames.size()]));
                        //匹配schema.xml中的<dynamicField name="*_attr" type="string" indexed="true" stored="true" multiValued="true"/>
                        //index.setDynamicFields(attrMap.isEmpty() ? null : attrMap);
                        index.setIs_suit(false);
                        index.setSale_num(0);
                        index.setCreate_time(new Date());
                        //index.setLastModified(index.getCreateTime());
                        index.setSku(prodSku.getSkuId());
                        index.setGome_sku(prodSku.getGomeSkuid());//国美sku
                        index.setAdver("");
                        assemIndexByGoodInfo(index, prodSku);//商品信息
                        //assemIndexByPrice(index, prodSku);//价格
                        index.setGood_comment_rate(0);
                        //assemIndexByComment(index, prodSku);//好评率
                        //以下条件不符合 代表不是正常商品  无需创建索引
//                        if (index.getProduct_sale_price() == 0 )
//                            continue;
                        String[] strings = {"品牌_sku_attr", "颜色-模板2使用_sku_attr", "类别-模板2使用_sku_attr","sku级枚举_sku_attr"
                        ,"sku级多选_sku_attr"};
                        index.setAttr_name(index.getAttr_name()==null ? strings : strings);
                        indexs.add(index);
                        //套购
                        //asyncSuitProdBySku(prodSku, indexs, index);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Gome SuperMarket "+prodSpu.getSpuId() + " spu同步失败：" + e.getMessage());

                } finally {
                    logger.info("Gome SuperMarket spu：" + prodSpu.getSpuId() + "同步结束");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        logger.info("Gome SuperMarket 第" + skip + "条数据同步个数" + limit + "结束");
        return indexs;
    }

    /**
     * 多值分类
     * @param prodSpu
     * @param index
     */
    private void assemCateMultiValued(ProdSpu prodSpu, GomehigoIndex index) {
        DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
        ProdSpuCateRelationExample condition = new ProdSpuCateRelationExample();
        ProdSpuCateRelationExample.Criteria criteria = condition.createCriteria();
        criteria.andSpuIdEqualTo(prodSpu.getSpuId());
        List<ProdSpuCateRelation> prodSpuCateRelationList = prodSpuCateRelationMapper.selectByExample(condition);
        List<String> cats = new ArrayList<>();
        Set<String> firstCats = new HashSet<>();
        Set<String> secondCats = new HashSet<>();

       if (prodSpuCateRelationList!=null){
           for (ProdSpuCateRelation cateRelation: prodSpuCateRelationList) {
               DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
               ProdCategory firstCategory = prodCategoryMapper.selectByPrimaryKey(Integer.parseInt(cateRelation.getFirstLevelCateId()));
               ProdCategory secondCategory = prodCategoryMapper.selectByPrimaryKey(Integer.parseInt(cateRelation.getSecondLevelCateId()));
               //ProdCategory thitdCategory = prodCategoryMapper.selectByPrimaryKey(Integer.parseInt(cateRelation.getThirdLevelCateId()));
               cats.add(cateRelation.getFirstLevelCateId()+"_"+cateRelation.getSecondLevelCateId()+"_"+cateRelation.getThirdLevelCateId());
               firstCats.add(cateRelation.getFirstLevelCateId()+"_"+firstCategory.getCateName());//一级
               secondCats.add(cateRelation.getFirstLevelCateId()+"_"+cateRelation.getSecondLevelCateId()+"_"+secondCategory.getCateName());//二级

           }

       }
//       index.setCats(cats.toArray(new String[]{}));
//       index.setCategoryFirsts(firstCats.toArray(new String[]{}));
//       index.setCategorySeconds(secondCats.toArray(new String[]{}));

    }

    private Map<String,Object> assemAttrMap(ProdSku prodSku, List<String> attrNames) throws Exception {

        Map<String,Object> attrMap = new HashMap();
        try {
            logger.info("INFO:商品属性信息同步开始：sku:" + prodSku.getSkuId() + "spu:" + prodSku.getSpuId());
            Map<String, Object> param = new HashMap<>();
            param.put(Constants.METHOD, "product.getGoodsInfo");
            param.put(Constants.PARAM_SKU_NO, prodSku.getSkuId());
            String prodInfo = HttpUtils.sendGet(PropertyUtils.API_SERVER_ADDR, HttpUtils.assemParam(param));
            JSONObject jsonObject = JSONObject.parseObject(prodInfo);
            if ("200".equals(jsonObject.getJSONObject("result").getString("code"))){

                JSONObject data = jsonObject.getJSONObject("data");
                if (data!=null){
                    JSONObject productDetails = data.getJSONObject("productDetails");
                    if (productDetails!=null){
                        JSONArray prdSpecArr = productDetails.getJSONArray("prdSpec");
                        for (int i = 0; i <prdSpecArr.size() ; i++) {
                            JSONObject prdSpe = (JSONObject)prdSpecArr.get(i);
                            if (prdSpe!=null){
                                JSONArray modeDataList = prdSpe.getJSONArray("modeData");
                                for (int j = 0; j <modeDataList.size() ; j++) {
                                    JSONObject modeData =(JSONObject) modeDataList.get(j);
                                    String attrName = modeData.getString("modeTxt");
                                    String attrValue= modeData.getString("modeInfo");
                                    String newAttrName = attrName+Constants._SKU_ATTR;
                                    attrMap.put(newAttrName,new String[]{attrValue});
                                    attrNames.add(newAttrName);
                                }
                            }
                        }

                    }
                }
            }else {
                logger.error("ERROR:request product goods info from gomehigo server exception! spu：" + prodSku.getSpuId() + "sku：" + prodSku.getSkuId());
            }
            return  attrMap;
        }catch (Exception e){
            logger.error("商品详情服务调用异常，spu：" + prodSku.getSpuId() + "sku：" + prodSku.getSkuId() + e.getMessage());
            throw new Exception();
        }finally {
            logger.info("INFO:商品属性信息同步结束：sku:" + prodSku.getSkuId() + "spu:" + prodSku.getSpuId());
        }

    }

   /* private void assemIndexSkuType(ProdSku prodSku, GomehigoIndex index) {
        logger.info("同步sku活动类型开始 sku:" + prodSku.getSkuId());
        int skuType = 0;
        String json = cacheClient.get(Constants.ACTIVITY_KEY);
        if (json != null && !json.equals("")) {
            JSONArray skuTypeArr = JSONObject.parseObject(json).getJSONArray(prodSku.getSkuId());
            if (skuTypeArr != null) {
                for (int indx = 0; indx < skuTypeArr.size(); indx++) {
                    Integer acType = skuTypeArr.getInteger(indx);
                    skuType = skuType + EnumEntity.activity.getV2ByV1(acType);
                }
            }
        }
        index.setSkuType(skuType);
        logger.info("同步sku活动类型结束 sku:" + prodSku.getSkuId() + " 类型为：" + skuType);
    }*/



    private void assemFgCate(ProdSpu prodSpu, GomehigoIndex index) {
        DataSourceContextHolder.setDbType(DataSourceType.CMS_DATA_SOURCE);
        CmsCategoryRelationExample cmsCategoryRelationExample = new CmsCategoryRelationExample();
        if (prodSpu.getCateId() != null) {
            cmsCategoryRelationExample.createCriteria().andProdCategoryIdEqualTo(prodSpu.getCateId());
            List<CmsCategoryRelation> cmsCategoryRelations = cmsCategoryRelationMapper.selectByExample(cmsCategoryRelationExample);
            List<String> thirdIds = new ArrayList<>();
            List<String> thirdNames = new ArrayList<>();
            List<String> secondIds = new ArrayList<>();
            List<String> secondNames = new ArrayList<>();
            List<String> firstIds = new ArrayList<>();
            ArrayList<String> firstNames = new ArrayList<>();
            for (CmsCategoryRelation relation : cmsCategoryRelations) {
                Integer categoryId = relation.getCmsCategoryId();
                CmsCategory third = cmsCategoryMapper.selectByPrimaryKey(categoryId);
                CmsCategory second = cmsCategoryMapper.selectByPrimaryKey(third.getCmsCateParentid());
                CmsCategory first = cmsCategoryMapper.selectByPrimaryKey(second.getCmsCateParentid());
                thirdIds.add(categoryId.toString());
                thirdNames.add(third.getCmsCateName());
                secondIds.add(second.getCmsCateId().toString());
                secondNames.add(second.getCmsCateName());
                firstIds.add(first.getCmsCateId().toString());
                firstNames.add(first.getCmsCateName());
            }
//            index.setFgCatFirst(firstNames.toArray());
//            index.setFgCatFirstId(firstIds.toArray());
//            index.setFgCatSecond(secondNames.toArray());
//            index.setFgCatSecondId(secondIds.toArray());
//            index.setFgCatThird(thirdNames.toArray());
//            index.setFgCatThirdId(thirdIds.toArray());
        }
    }

//    private String assemHref(WebHref webHref) {
//        String href = String.format("//www.gomehigo.com/s/%s-%s-%s-%d-%d-%s-%s-%s.html", webHref.getFgFirstId(), webHref.getFgSecondId(), webHref.getFgThirdId(), webHref.getBrandId(), webHref.getCountryId(), webHref.getProdThirdId(), webHref.getPrice(), webHref.getAttr());
//        return href;
//    }

    private Map<String, Object> assemAttrMap(ProdSpu prodSpu, ProdSku sku, List attrNames) {
        Map<String, Object> attrMap = new LinkedHashMap<>();
        // assemCateAttr(prodSpu, attrMap);
        //cms_solr_filter_cate_attr  前后台分类管理页面 可以对分类对应的属性进行管理
        //获取spu所属的分类的 所有属性
        List<CmsSolrFilterCateAttr> cateAttrs = getProdFilterCateAttrId(prodSpu);
        //获取并组装spu的属性信息
        assemSpuAttr(prodSpu, attrMap);
        //获取并且组装sku属性信息
        assemSkuAttr(sku, attrMap);
        //对刷选项进行排序
        //由于分类属性最全，可以在后台页面管理（删除，增加），以分类属性为基准过滤-->（一个spu属性以及这个spu下的所有sku的属性）
        LinkedHashMap<String, Object> sortAttrMap = new LinkedHashMap<>();
        for (CmsSolrFilterCateAttr cateAttr : cateAttrs) {
            Iterator<String> iterator = attrMap.keySet().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                if (key.startsWith(cateAttr.getFilterAttId().toString())) {
                    sortAttrMap.put(key, attrMap.get(key));
                    attrNames.add(key);
                }
            }
        }
        return sortAttrMap;
    }

    private List<CmsSolrFilterCateAttr> getProdFilterCateAttrId(ProdSpu prodSpu) {
        DataSourceContextHolder.setDbType(DataSourceType.CMS_DATA_SOURCE);
        Integer cateId = prodSpu.getCateId();
        CmsSolrFilterCateAttrExample cmsSolrFilterCateAttrExample = new CmsSolrFilterCateAttrExample();
        cmsSolrFilterCateAttrExample.setOrderByClause(" filter_att_score desc ");
        CmsSolrFilterCateAttrExample.Criteria criteria = cmsSolrFilterCateAttrExample.createCriteria();
        criteria.andFilterCateIdEqualTo(cateId);
        List<CmsSolrFilterCateAttr> cmsSolrFilterCateAttrs = cmsSolrFilterCateAttrMapper.selectByExample(cmsSolrFilterCateAttrExample);

        return cmsSolrFilterCateAttrs;
    }

    private void assemSkuAttr(ProdSku sku, Map<String, Object> attrMap) {
        DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
        if (sku != null && sku.getSkuId() != null) {
            List<ProdSkuAttribute> skuAttrs = prodSkuAttributeMapper.selectBySkuId(sku.getSkuId());
            for (ProdSkuAttribute prodSkuAttribute : skuAttrs) {
                if (prodSkuAttribute.getAttValueId() != null) {
                    List<String> attrValues = assemAttrValues(prodSkuAttribute.getAttValueId());
                    //属性值ID__sku_attr 221_spu_attr
                    attrMap.put(prodSkuAttribute.getAttId() + Constants._SKU_ATTR, attrValues.toArray());
                }
            }
        }
    }

    private void assemSpuAttr(ProdSpu prodSpu, Map<String, Object> attrMap) {
        DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
        List<ProdSpuAttribute> spuAttrs = prodSpuAttributeMapper.selectBySpuId(prodSpu.getSpuId());
        for (ProdSpuAttribute spuAttr : spuAttrs) {
            if (spuAttr.getAttValueId() != null) {
                List<String> attrValues = assemAttrValues(spuAttr.getAttValueId());
                //属性ID__spu_attr
                attrMap.put(spuAttr.getAttId() + Constants._SPU_ATTR, attrValues.toArray());
            }
        }
    }

    private List<String> assemAttrValues(Integer attrValueId) {
        DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
        ProdAttributeValue attrValue = ProdAttributeValueMapper.selectByPrimaryKey(attrValueId);
        List<String> attrValues = new ArrayList<>();
        //属性值id_属性值 399_进口
        StringBuilder sb = new StringBuilder();
        sb.append(attrValue.getAttValueId());
        sb.append("_");
        sb.append(attrValue.getAttValue());
        attrValues.add(sb.toString());
        return attrValues;
    }

    private void assemCateAttr(ProdSpu prodSpu, Map<String, Object> attrMap) {
        DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
        List<ProdCateAtt> cateAttrs = prodCateAttMapper.selectByCateId(prodSpu.getCateId());
        for (ProdCateAtt prodCateAtt : cateAttrs) {
            List<String> attrValues = assemAttrValues(prodCateAtt.getAttId());
            attrMap.put(prodCateAtt.getAttId() + Constants._CATE_ATTR, attrValues.toArray());
        }
    }


    private void assemIndexByPrice(GomehigoIndex index, ProdSku sku) throws Exception {
        logger.info("Gome SuperMarket sku spu价格调用同步开始：sku:" + sku.getSkuId() + "spu:" + sku.getSpuId());
        try {
            Map<String, Object> param = new HashMap<>();
//            param.put(Constants.METHOD, "product.getMobileProdPrice");
           // param.put(Constants.METHOD, "product.getGoodsPrice");
            param.put(Constants.PARAM_SKU_NO, sku.getSkuId());
            String priceResult = HttpUtils.sendGet(PropertyUtils.API_SERVER_ADDR+"/goods/getGoodsDynamicInfo", HttpUtils.assemParam(param));
            JSONObject jsonRe = JSONObject.parseObject(priceResult);
            if (jsonRe.getJSONObject("result").getInteger("code") == 200) {
                JSONObject data = jsonRe.getJSONObject("data");
                JSONObject skuObject = data.getJSONObject(sku.getSkuId());
                JSONObject price = skuObject.getJSONObject("price");
                index.setProduct_proto_price(price.get("originalPrice") != null ? new BigDecimal(price.get("originalPrice").toString()).doubleValue() : null);
                index.setProduct_sale_price(price.get("price") != null ? new BigDecimal(price.get("price").toString()).doubleValue() : null);
                //pc
                index.setProduct_sale_price(price.get("pcPrice") != null ? new BigDecimal(price.get("pcPrice").toString()).doubleValue() : null);
               /* JSONObject wapPrice = skuObject.getJSONObject("wap");
                index.setProductProtoPrice(wapPrice.get("originalPrice") != null ? new BigDecimal(wapPrice.get("originalPrice").toString()).doubleValue() : null);
                index.setProductSalePrice(wapPrice.get("price") != null ? new BigDecimal(wapPrice.get("price").toString()).doubleValue() : null);
                index.setIsMobilePrice(wapPrice.getString("priceType").equals("PALMPRICE"));

                JSONObject pcPrice = skuObject.getJSONObject("pc");
                index.setProductPcProtoPrice(pcPrice.get("originalPrice") != null ? new BigDecimal(pcPrice.get("originalPrice").toString()).doubleValue() : null);
                index.setProductPcSalePrice(pcPrice.get("price") != null ? new BigDecimal(pcPrice.get("price").toString()).doubleValue() : null);*/
            } else {
                logger.error("ERROR:request price from Gome SuperMarket server exception!");
            }
        } catch (Exception e) {
            logger.error("Gome SuperMarket 商品价格服务调用异常，spu：" + sku.getSpuId() + "sku：" + sku.getSkuId() + e.getMessage());
            throw new Exception();
        }
        logger.info("Gome SuperMarket sku spu价格调用同步结束：sku:" + sku.getSkuId() + "spu:" + sku.getSpuId());
    }

    private void assemIndexByGoodInfo(GomehigoIndex index, ProdSku sku) throws Exception {
        logger.info("Gome SuperMarket sku spu信息调用同步开始：sku:" + sku.getSkuId() + "spu:" + sku.getSpuId());
        try {
            Map<String, Object> param = new HashMap<>();
            //param.put(Constants.METHOD, "product.getGoodsInfo");
            param.put(Constants.PARAM_SKU_NO, sku.getSkuId());
            String goodsResult = HttpUtils.sendGet(PropertyUtils.API_SERVER_ADDR, HttpUtils.assemParam(param));
            JSONObject jsonRe = JSONObject.parseObject(goodsResult);
            if (jsonRe != null && jsonRe.getJSONObject("result").getInteger("code") == 200) {
                JSONObject data = jsonRe.getJSONObject("data");
                if (data==null){
                    return;
                }
                JSONObject goods = data.getJSONObject("goods");
               if (goods!=null){
                   index.setShop(goods.getString("shopName"));
                   if (index.getBrand_id() != null) {
                       index.setBrand(index.getBrand_id() + "_" + (goods.getString("brand").equals("") ? "无品牌" : goods.getString("brand")));
                   }
                   JSONArray imgArr = goods.getJSONArray("goodsImgs");
                   index.setProduct_img(imgArr != null && imgArr.size() > 0 ? imgArr.getJSONObject(0).getJSONObject("h").getString("src") : "");
                   index.setAdver(goods.get("adver") != null ? goods.getString("adver") : "");
                   int status = goods.getIntValue("status");
                   int quantity = goods.getIntValue("quantity");
                   index.setIs_self(status == 5 ? true : false);
                   index.setIs_in_store(quantity== 0 ? false : true);
                   //价格
                   index.setProduct_sale_price(goods.getJSONObject("price").getDouble("currentPrice"));


                   List<String> attrNames = new ArrayList<String>();
                   //属性
                   Map<String, Object> attrMap = new HashMap<>();
                   if (goods!=null){
                       JSONObject productDetails = goods.getJSONObject("productDetails");
                       if (productDetails!=null){
                           JSONArray prdSpecArr = productDetails.getJSONArray("prdSpec");
                           for (int i = 0; i <prdSpecArr.size() ; i++) {
                               JSONObject prdSpe = (JSONObject)prdSpecArr.get(i);
                               if (prdSpe!=null){
                                   JSONArray modeDataList = prdSpe.getJSONArray("modeData");
                                   for (int j = 0; j <modeDataList.size() ; j++) {
                                       JSONObject modeData =(JSONObject) modeDataList.get(j);
                                       String attrName = modeData.getString("modeTxt");
                                       String attrValue= modeData.getString("modeInfo");
                                       String newAttrName = attrName+Constants._SKU_ATTR;
                                       attrMap.put(newAttrName,new String[]{attrValue});
                                       attrNames.add(newAttrName);
                                   }
                               }
                           }

                       }
                   }
                   //搜索聚合项目 attr_name
                   String[] strings = {"品牌_sku_attr", "商品类别_sku_attr", "面料_sku_attr"};
                   index.setAttr_name(attrNames.isEmpty() ? strings : attrNames.toArray(new String[attrNames.size()]));
                   //匹配schema.xml中的<dynamicField name="*_attr" type="string" indexed="true" stored="true" multiValued="true"/>
                   index.setDynamicFields(attrMap.isEmpty() ? null : attrMap);
               }
               // 促销
                List<String> activityIdList = new ArrayList<>();
                List<String> activityNameList = new ArrayList<>();
                JSONArray activityList = data.getJSONArray("activityList");
                if (activityList!=null){
                    for (int i = 0; i <activityList.size() ; i++) {
                        JSONObject activityInfo = activityList.getJSONObject(i);
                        Long activityId = activityInfo.getLong("activityId");
                        String activityName = activityInfo.getString("activityName");
                        activityIdList.add(activityId+"");
                        activityNameList.add(activityId+"_"+activityName);
                    }
                }
//                index.setActivityIds(activityIdList.toArray(new String[activityIdList.size()]));
//                index.setActivityNames(activityNameList.toArray(new String[activityNameList.size()]));
            } else {
                logger.error("ERROR:request goodInfo from Gome SuperMarket server exception! " + sku.getSkuId());
            }
        } catch (Exception e) {
            logger.error("Gome SuperMarket 商品详情服务调用异常，spu：" + sku.getSpuId() + "sku：" + sku.getSkuId() + e.getMessage());
            throw new Exception();
        }
        logger.info("Gome SuperMarket sku spu信息调用同步结束：sku:" + sku.getSkuId() + "spu:" + sku.getSpuId());
    }

    private void assemCate(ProdSpu prodSpu, GomehigoIndex index) {
        DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
        ProdCategory thirdCategory = prodCategoryMapper.selectByPrimaryKey(prodSpu.getCateId());
        if (thirdCategory.getCateId() != null) {
            index.setCategory_third_id(thirdCategory.getCateId().toString());
            index.setCategory_third(thirdCategory.getCateId() + "_" + thirdCategory.getCateName());
            if (thirdCategory != null) {
                ProdCategory secondCateGory = prodCategoryMapper.selectByPrimaryKey(thirdCategory.getParentCateId());
                if (secondCateGory != null) {
                    index.setCategory_second_id(secondCateGory.getCateId().toString());
                    index.setCategory_second(secondCateGory.getCateId() + "_" + secondCateGory.getCateName());

                    ProdCategory firstCateGory = prodCategoryMapper.selectByPrimaryKey(secondCateGory.getParentCateId());
                    if (firstCateGory != null) {
                        index.setCategory_first_id(firstCateGory.getCateId().toString());
                        index.setCategory_first(firstCateGory.getCateId() + "_" + firstCateGory.getCateName());
                    }

                }
            }
        }

    }

    @Override
    public int getMaxNum() {
        DataSourceContextHolder.setDbType(DataSourceType.PROD_DATA_SOURCE);
        return prodSpuMapper.getMaxCount();
    }



    public static void main(String[] args) {
        System.out.println(EnumEntity.activity.getV2ByV1(2));
    }

}
