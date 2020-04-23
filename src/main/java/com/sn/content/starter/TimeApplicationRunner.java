package com.sn.content.starter;

import com.sn.content.service.ChaptersService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * @author songning
 * @date 2020/4/21
 * description
 */
@Component
@Slf4j
public class TimeApplicationRunner implements ApplicationRunner {

    @Autowired
    private ChaptersService chaptersService;

    @Override
    public void run(ApplicationArguments args) {
        try {
            chaptersService.updateContent();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("updateNonContent fail: {}", e.getMessage());
        }
    }
}
