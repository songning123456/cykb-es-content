package com.sn.content.entity;

import lombok.*;

/**
 * @author: songning
 * @date: 2020/3/9 22:30
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Data
public class Chapters {

    private String chapter;

    private String content;

    private String novelsId;

    private String updateTime;

    private String contentUrl;
}
