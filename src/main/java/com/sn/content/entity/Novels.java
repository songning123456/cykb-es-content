package com.sn.content.entity;

import lombok.*;

/**
 * @author: songning
 * @date: 2020/3/9 22:17
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Data
public class Novels {

    private String title;

    private String author;

    private String category;

    private String introduction;

    private String latestChapter;

    private String coverUrl;

    private String updateTime;

    private Long createTime;

    private String sourceUrl;

    private String sourceName;

    private String status;
}
