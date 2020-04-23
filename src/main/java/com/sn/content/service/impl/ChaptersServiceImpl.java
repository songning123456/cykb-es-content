package com.sn.content.service.impl;

import com.sn.content.elasticsearch.dao.ElasticSearchDao;
import com.sn.content.elasticsearch.entity.ElasticSearch;
import com.sn.content.entity.Chapters;
import com.sn.content.service.ChaptersService;
import com.sn.content.util.HttpUtil;
import io.searchbox.core.SearchResult;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author: songning
 * @date: 2020/4/6 11:09
 */
@Slf4j
@Service
public class ChaptersServiceImpl implements ChaptersService {

    @Autowired
    private ElasticSearchDao elasticSearchDao;

    @Override
    public void updateContent() {
        int recordStartNo = 0;
        int pageRecordNum = 10000;
        long total = 10000;
        try {
            Map<String, Object> termParams = new HashMap<String, Object>(2) {{
                put("content", "暂无资源...");
            }};
            while (pageRecordNum <= total) {
                ElasticSearch chaptersEsSearch = ElasticSearch.builder().index("chapters_index").type("chapters").from(recordStartNo).size(pageRecordNum).sort("updateTime").order("asc").build();
                SearchResult searchResult = elasticSearchDao.mustTermRangeQueryWithTotal(chaptersEsSearch, termParams, null);
                total = searchResult.getTotal();
                List<SearchResult.Hit<Object, Void>> src = searchResult.getHits(Object.class);
                for (SearchResult.Hit<Object, Void> item : src) {
                    try {
                        String chaptersId = item.id;
                        String content = "暂无资源...";
                        String chapter = String.valueOf(((Map) item.source).get("chapter"));
                        String contentUrl = String.valueOf(((Map) item.source).get("contentUrl"));
                        try {
                            Document contentDoc = HttpUtil.getHtmlFromUrl(contentUrl, true);
                            content = contentDoc.getElementById("content").html();
                        } catch (Exception e) {
                            e.printStackTrace();
                            log.error("更新(content)文本内容chaptersId: {} fail", chaptersId);
                        }
                        String novelsId = String.valueOf(((Map) item.source).get("novelsId"));
                        String updateTime = String.valueOf(((Map) item.source).get("updateTime"));
                        Chapters chapters = Chapters.builder().chapter(chapter).content(content).contentUrl(contentUrl).novelsId(novelsId).updateTime(updateTime).build();
                        Map entityMap = (Map) elasticSearchDao.findById(chaptersEsSearch, chaptersId);
                        if ("暂无资源...".equals(entityMap.get("content"))) {
                            elasticSearchDao.update(chaptersEsSearch, chaptersId, chapters);
                            log.info("当前小说novelsId: {}; 更新章节chapter: {}", novelsId, chapter);
                        } else {
                            log.info("novelsId: {}, 中间文本内容已经更新chapter: {}", novelsId, chapter);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("更新chapterId:{} fail", item.id);
                    }
                }
                recordStartNo = recordStartNo + 10000;
                pageRecordNum = pageRecordNum + 10000;
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.info("~~~查询所有内容失败!~~~");
        }

    }

    private List<Map<String, Object>> findByNovelsId(String novelsId) {
        ElasticSearch elasticSearch = ElasticSearch.builder().index("chapters_index").type("chapters").sort("updateTime").order("asc").build();
        Map<String, Object> termParams = new HashMap<String, Object>(2) {{
            put("novelsId", novelsId);
        }};
        List<SearchResult.Hit<Object, Void>> src = new ArrayList<>();
        try {
            src = elasticSearchDao.mustTermRangeQuery(elasticSearch, termParams, null);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("findByNovelsId-fail: {}", e.getMessage());
        }
        List<Map<String, Object>> target = new ArrayList<>();
        for (SearchResult.Hit<Object, Void> objectVoidHit : src) {
            Map<String, Object> temp = new HashMap<>(2);
            temp.put("chapter", ((Map) objectVoidHit.source).get("chapter"));
            temp.put("updateTime", ((Map) objectVoidHit.source).get("updateTime"));
            target.add(temp);
        }
        return target;
    }
}
