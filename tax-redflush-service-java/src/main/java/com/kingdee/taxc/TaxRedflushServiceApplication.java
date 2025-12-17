package com.kingdee.taxc;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.kingdee.taxc.mapper")
public class TaxRedflushServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaxRedflushServiceApplication.class, args);
    }
}
