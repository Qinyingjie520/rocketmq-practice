package com.atguigu.rocketmq;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class RocketmqDemo1Application {

    @Autowired
    static JdbcTemplate jdbcTemplate;

    public static void main(String[] args) {

        SpringApplication.run(RocketmqDemo1Application.class, args);

        System.out.println("启动成功");
        System.out.println("JdbcTemplate = "+jdbcTemplate);
    }

}
