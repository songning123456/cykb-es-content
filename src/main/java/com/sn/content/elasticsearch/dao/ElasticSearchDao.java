package com.sn.content.elasticsearch.dao;

import com.sn.content.elasticsearch.entity.ElasticSearch;
import com.sn.content.elasticsearch.entity.Range;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.core.search.aggregation.TermsAggregation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author songning
 * @date 2020/4/15
 * description
 */
@Component
@Slf4j
public class ElasticSearchDao {

    @Autowired
    private JestClient jestClient;

    public <T> JestResult save(ElasticSearch elasticSearch, T entity) throws Exception {
        if (entity == null) {
            throw new Exception("elasticSearch-save 参数异常");
        }
        Index action = new Index.Builder(entity).index(elasticSearch.getIndex()).type(elasticSearch.getType()).build();
        JestResult jestResult = jestClient.execute(action);
        if (!jestResult.isSucceeded()) {
            throw new Exception(jestResult.getErrorMessage());
        }
        return jestResult;
    }

    public <T> void bulk(ElasticSearch elasticSearch, List<T> list) throws Exception {
        if (list == null || list.isEmpty()) {
            throw new Exception("elasticSearch-bulk 参数异常");
        }
        List<Index> actionList = new ArrayList<>();
        list.forEach(item -> actionList.add(new Index.Builder(item).build()));
        Bulk bulk = new Bulk.Builder().defaultIndex(elasticSearch.getIndex()).defaultType(elasticSearch.getType()).addAction(actionList).build();
        JestResult jestResult = jestClient.execute(bulk);
        if (!jestResult.isSucceeded()) {
            throw new Exception(jestResult.getErrorMessage());
        }
    }

    public <T> void update(ElasticSearch elasticSearch, String esId, T entity) throws Exception {
        if (StringUtils.isEmpty(esId) || entity == null) {
            throw new Exception("elasticSearch-update 参数异常");
        }
        Index index = new Index.Builder(entity).index(elasticSearch.getIndex()).type(elasticSearch.getType()).id(esId).build();
        JestResult jestResult = jestClient.execute(index);
        if (!jestResult.isSucceeded()) {
            throw new Exception(jestResult.getErrorMessage());
        }
    }

    public Object findById(ElasticSearch elasticSearch, String esId) throws Exception {
        if (StringUtils.isEmpty(esId)) {
            throw new Exception("elasticSearch-findById 参数异常");
        }
        Get get = new Get.Builder(elasticSearch.getIndex(), esId).type(elasticSearch.getType()).build();
        JestResult result = jestClient.execute(get);
        return result.getSourceAsObject(Object.class);
    }

    public void mustTermsDelete(ElasticSearch elasticSearch, Map<String, String[]> termsParams) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (termsParams != null && !termsParams.isEmpty()) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (Map.Entry<String, String[]> item : termsParams.entrySet()) {
                boolQueryBuilder.must(QueryBuilders.termsQuery(item.getKey(), item.getValue()));
            }
            searchSourceBuilder.query(boolQueryBuilder);
        }
        DeleteByQuery deleteByQuery = new DeleteByQuery.Builder(searchSourceBuilder.toString()).addIndex(elasticSearch.getIndex()).addType(elasticSearch.getType()).build();
        JestResult jestResult = jestClient.execute(deleteByQuery);
        if (!jestResult.isSucceeded()) {
            throw new Exception(jestResult.getErrorMessage());
        }
    }

    public List<SearchResult.Hit<Object, Void>> mustTermRangeQuery(ElasticSearch elasticSearch, Map<String, Object> termParams, List<Range> rangeList) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (termParams != null && !termParams.isEmpty()) {
            for (Map.Entry<String, Object> item : termParams.entrySet()) {
                boolQueryBuilder.must(QueryBuilders.termQuery(item.getKey(), item.getValue()));
            }
        }
        if (rangeList != null && !rangeList.isEmpty()) {
            for (Range range : rangeList) {
                if (StringUtils.isEmpty(range.getMin()) && StringUtils.isEmpty(range.getMax())) {
                    continue;
                }
                if (StringUtils.isEmpty(range.getGtOrGte()) && StringUtils.isEmpty(range.getLtOrLte())) {
                    continue;
                }
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(range.getRangeName());
                if (!StringUtils.isEmpty(range.getMin()) && "gt".equals(range.getGtOrGte())) {
                    rangeQueryBuilder.gt(range.getMin());
                } else if (!StringUtils.isEmpty(range.getMin()) && "gte".equals(range.getGtOrGte())) {
                    rangeQueryBuilder.gte(range.getMin());
                }
                if (!StringUtils.isEmpty(range.getMax()) && "lt".equals(range.getLtOrLte())) {
                    rangeQueryBuilder.lt(range.getMax());
                } else if (!StringUtils.isEmpty(range.getMax()) && "lte".equals(range.getLtOrLte())) {
                    rangeQueryBuilder.lte(range.getMax());
                }
                boolQueryBuilder.must(rangeQueryBuilder);
            }
        }
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.from(elasticSearch.getFrom()).size(elasticSearch.getSize());
        if (!StringUtils.isEmpty(elasticSearch.getSort()) && ("ASC".equals(elasticSearch.getOrder()) || "asc".equals(elasticSearch.getOrder()))) {
            searchSourceBuilder.sort(elasticSearch.getSort(), SortOrder.ASC);
        } else if (!StringUtils.isEmpty(elasticSearch.getSort()) && ("DESC".equals(elasticSearch.getOrder()) || "desc".equals(elasticSearch.getOrder()))) {
            searchSourceBuilder.sort(elasticSearch.getSort(), SortOrder.DESC);
        }
        String query = searchSourceBuilder.toString();
        Search search = new Search.Builder(query).addIndex(elasticSearch.getIndex()).addType(elasticSearch.getType()).build();
        SearchResult searchResult = jestClient.execute(search);
        return searchResult.getHits(Object.class);
    }

    public SearchResult mustTermRangeQueryWithTotal(ElasticSearch elasticSearch, Map<String, Object> termParams, List<Range> rangeList) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (termParams != null && !termParams.isEmpty()) {
            for (Map.Entry<String, Object> item : termParams.entrySet()) {
                boolQueryBuilder.must(QueryBuilders.termQuery(item.getKey(), item.getValue()));
            }
        }
        if (rangeList != null && !rangeList.isEmpty()) {
            for (Range range : rangeList) {
                if (StringUtils.isEmpty(range.getMin()) && StringUtils.isEmpty(range.getMax())) {
                    continue;
                }
                if (StringUtils.isEmpty(range.getGtOrGte()) && StringUtils.isEmpty(range.getLtOrLte())) {
                    continue;
                }
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(range.getRangeName());
                if (!StringUtils.isEmpty(range.getMin()) && "gt".equals(range.getGtOrGte())) {
                    rangeQueryBuilder.gt(range.getMin());
                } else if (!StringUtils.isEmpty(range.getMin()) && "gte".equals(range.getGtOrGte())) {
                    rangeQueryBuilder.gte(range.getMin());
                }
                if (!StringUtils.isEmpty(range.getMax()) && "lt".equals(range.getLtOrLte())) {
                    rangeQueryBuilder.lt(range.getMax());
                } else if (!StringUtils.isEmpty(range.getMax()) && "lte".equals(range.getLtOrLte())) {
                    rangeQueryBuilder.lte(range.getMax());
                }
                boolQueryBuilder.must(rangeQueryBuilder);
            }
        }
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.from(elasticSearch.getFrom()).size(elasticSearch.getSize());
        if (!StringUtils.isEmpty(elasticSearch.getSort()) && ("ASC".equals(elasticSearch.getOrder()) || "asc".equals(elasticSearch.getOrder()))) {
            searchSourceBuilder.sort(elasticSearch.getSort(), SortOrder.ASC);
        } else if (!StringUtils.isEmpty(elasticSearch.getSort()) && ("DESC".equals(elasticSearch.getOrder()) || "desc".equals(elasticSearch.getOrder()))) {
            searchSourceBuilder.sort(elasticSearch.getSort(), SortOrder.DESC);
        }
        String query = searchSourceBuilder.toString();
        Search search = new Search.Builder(query).addIndex(elasticSearch.getIndex()).addType(elasticSearch.getType()).build();
        SearchResult searchResult = jestClient.execute(search);
        return searchResult;
    }

    public List<SearchResult.Hit<Object, Void>> mustTermsRangeQuery(ElasticSearch elasticSearch, Map<String, String[]> termsParams, List<Range> rangeList) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (termsParams != null && !termsParams.isEmpty()) {
            for (Map.Entry<String, String[]> item : termsParams.entrySet()) {
                boolQueryBuilder.must(QueryBuilders.termsQuery(item.getKey(), item.getValue()));
            }
        }
        if (rangeList != null && !rangeList.isEmpty()) {
            for (Range range : rangeList) {
                if (StringUtils.isEmpty(range.getMin()) && StringUtils.isEmpty(range.getMax())) {
                    continue;
                }
                if (StringUtils.isEmpty(range.getGtOrGte()) && StringUtils.isEmpty(range.getLtOrLte())) {
                    continue;
                }
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(range.getRangeName());
                if (!StringUtils.isEmpty(range.getMin()) && "gt".equals(range.getGtOrGte())) {
                    rangeQueryBuilder.gt(range.getMin());
                } else if (!StringUtils.isEmpty(range.getMin()) && "gte".equals(range.getGtOrGte())) {
                    rangeQueryBuilder.gte(range.getMin());
                }
                if (!StringUtils.isEmpty(range.getMax()) && "lt".equals(range.getLtOrLte())) {
                    rangeQueryBuilder.lt(range.getMax());
                } else if (!StringUtils.isEmpty(range.getMax()) && "lte".equals(range.getLtOrLte())) {
                    rangeQueryBuilder.lte(range.getMax());
                }
                boolQueryBuilder.must(rangeQueryBuilder);
            }
        }
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.from(elasticSearch.getFrom()).size(elasticSearch.getSize());
        if (!StringUtils.isEmpty(elasticSearch.getSort()) && ("ASC".equals(elasticSearch.getOrder()) || "asc".equals(elasticSearch.getOrder()))) {
            searchSourceBuilder.sort(elasticSearch.getSort(), SortOrder.ASC);
        } else if (!StringUtils.isEmpty(elasticSearch.getSort()) && ("DESC".equals(elasticSearch.getOrder()) || "desc".equals(elasticSearch.getOrder()))) {
            searchSourceBuilder.sort(elasticSearch.getSort(), SortOrder.DESC);
        }
        String query = searchSourceBuilder.toString();
        Search search = new Search.Builder(query).addIndex(elasticSearch.getIndex()).addType(elasticSearch.getType()).build();
        SearchResult searchResult = jestClient.execute(search);
        return searchResult.getHits(Object.class);
    }

    public List<SearchResult.Hit<Object, Void>> mustTermShouldWildCardQuery(ElasticSearch elasticSearch, Map<String, Object> termParams, Map<String, Object> wildCardParams, List<Range> rangeList) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (termParams != null && !termParams.isEmpty()) {
            for (Map.Entry<String, Object> item : termParams.entrySet()) {
                boolQueryBuilder.must(QueryBuilders.termQuery(item.getKey(), item.getValue()));
            }
        }
        if (wildCardParams != null && !wildCardParams.isEmpty()) {
            BoolQueryBuilder innerBoolQueryBuilder = QueryBuilders.boolQuery();
            for (Map.Entry<String, Object> item : wildCardParams.entrySet()) {
                innerBoolQueryBuilder.should(QueryBuilders.wildcardQuery(item.getKey(), "*" + item.getValue() + "*"));
            }
            boolQueryBuilder.must(innerBoolQueryBuilder);
        }
        if (rangeList != null && !rangeList.isEmpty()) {
            for (Range range : rangeList) {
                if (StringUtils.isEmpty(range.getMin()) && StringUtils.isEmpty(range.getMax())) {
                    continue;
                }
                if (StringUtils.isEmpty(range.getGtOrGte()) && StringUtils.isEmpty(range.getLtOrLte())) {
                    continue;
                }
                RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(range.getRangeName());
                if (!StringUtils.isEmpty(range.getMin()) && "gt".equals(range.getGtOrGte())) {
                    rangeQueryBuilder.gt(range.getMin());
                } else if (!StringUtils.isEmpty(range.getMin()) && "gte".equals(range.getGtOrGte())) {
                    rangeQueryBuilder.gte(range.getMin());
                }
                if (!StringUtils.isEmpty(range.getMax()) && "lt".equals(range.getLtOrLte())) {
                    rangeQueryBuilder.lt(range.getMax());
                } else if (!StringUtils.isEmpty(range.getMax()) && "lte".equals(range.getLtOrLte())) {
                    rangeQueryBuilder.lte(range.getMax());
                }
                boolQueryBuilder.must(rangeQueryBuilder);
            }
        }
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.from(elasticSearch.getFrom()).size(elasticSearch.getSize());
        if (!StringUtils.isEmpty(elasticSearch.getSort()) && ("ASC".equals(elasticSearch.getOrder()) || "asc".equals(elasticSearch.getOrder()))) {
            searchSourceBuilder.sort(elasticSearch.getSort(), SortOrder.ASC);
        } else if (!StringUtils.isEmpty(elasticSearch.getSort()) && ("DESC".equals(elasticSearch.getOrder()) || "desc".equals(elasticSearch.getOrder()))) {
            searchSourceBuilder.sort(elasticSearch.getSort(), SortOrder.DESC);
        }
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(elasticSearch.getIndex()).addType(elasticSearch.getType()).build();
        SearchResult searchResult = jestClient.execute(search);
        return searchResult.getHits(Object.class);
    }

    public List<TermsAggregation.Entry> aggregationTermQuery(ElasticSearch elasticSearch, Map<String, Object> termParams, String aggField) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (termParams != null && !termParams.isEmpty()) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (Map.Entry<String, Object> item : termParams.entrySet()) {
                boolQueryBuilder.must(QueryBuilders.termQuery(item.getKey(), item.getValue()));
            }
            searchSourceBuilder.query(boolQueryBuilder);
        }
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(aggField + "Agg").field(aggField).size(elasticSearch.getSize());
        if ("asc".equals(elasticSearch.getOrder()) || "ASC".equals(elasticSearch.getOrder())) {
            termsAggregationBuilder.order(Terms.Order.aggregation("_count", true));
        } else if ("desc".equals(elasticSearch.getOrder()) || "DESC".equals(elasticSearch.getOrder())) {
            termsAggregationBuilder.order(Terms.Order.aggregation("_count", false));
        }
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchSourceBuilder.size(0);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(elasticSearch.getIndex()).addType(elasticSearch.getType()).build();
        SearchResult searchResult = jestClient.execute(search);
        return searchResult.getAggregations().getTermsAggregation(aggField + "Agg").getBuckets();
    }

    public List<TermsAggregation.Entry> aggregationTermSubQuery(ElasticSearch elasticSearch, Map<String, Object> termParams, String aggField, String subAggField) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (termParams != null && !termParams.isEmpty()) {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (Map.Entry<String, Object> item : termParams.entrySet()) {
                boolQueryBuilder.must(QueryBuilders.termQuery(item.getKey(), item.getValue()));
            }
            searchSourceBuilder.query(boolQueryBuilder);
        }
        searchSourceBuilder.aggregation(AggregationBuilders.terms(aggField + "Agg").field(aggField).size(elasticSearch.getSize())
                .order(Terms.Order.aggregation("total", false))
                .subAggregation(AggregationBuilders.count("total").field(aggField))
                .subAggregation(AggregationBuilders.terms(subAggField + "SubAgg").field(subAggField).size(elasticSearch.getSize())));
        searchSourceBuilder.size(0);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(elasticSearch.getIndex()).addType(elasticSearch.getType()).build();
        SearchResult searchResult = jestClient.execute(search);
        return searchResult.getAggregations().getTermsAggregation(aggField + "Agg").getBuckets();
    }
}
