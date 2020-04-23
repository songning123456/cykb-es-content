package com.sn.content.elasticsearch.entity;

import lombok.*;

/**
 * @author songning
 * @date 2020/4/15
 * description
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Range {

    @NonNull
    private String rangeName;

    private String gtOrGte;

    private String ltOrLte;

    private Object min;

    private Object max;
}
