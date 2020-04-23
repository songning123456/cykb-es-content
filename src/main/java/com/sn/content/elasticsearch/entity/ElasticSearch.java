package com.sn.content.elasticsearch.entity;

import lombok.*;

/**
 * @author songning
 * @date 2020/4/15
 * description
 */
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Data
public class ElasticSearch {

    @NonNull
    private String index;

    @NonNull
    private String type;

    @Builder.Default
    private Integer from = 0;

    @Builder.Default
    private Integer size = 10000;

    private String sort;

    @Builder.Default
    private String order = "asc";
}
