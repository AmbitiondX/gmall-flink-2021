package com.atguigu.gmall.gmallpublisher.controller;

import com.atguigu.gmall.gmallpublisher.service.impl.ProductStatsServiceImpl;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductStatsServiceImpl productStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") int date){

        if (date == 0) {
            getToday();
        }

        BigDecimal gmv = productStatsService.getGMV(date);

        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": " + gmv + " " +
                "}";

    }

    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long ts = System.currentTimeMillis();
        return Integer.parseInt(sdf.format(new Date(ts)));
    }
}
