package com.ucar.streamsuite;

import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.support.SpringBootServletInitializer;

import java.io.IOException;

/**
 * Description: 启动类
 * Created on 2018/1/18 下午3:09
 *
 */

@SpringBootApplication
@MapperScan("com.ucar.streamsuite.dao.mysql")
public class StreamSuiteMain extends SpringBootServletInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSuiteMain.class);


    public static void main(String[] args) throws IOException, InterruptedException {
        LOGGER.error("###################");

        SpringApplication app = new SpringApplication(StreamSuiteMain.class);
        app.setAdditionalProfiles();
        app.setBannerMode(Banner.Mode.CONSOLE);
        app.run(args);
        LOGGER.error("StreamSuiteMain ---started");
    }

}
