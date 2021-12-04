package com.atguigu.gmall.gmallpublisher.controller;

import com.atguigu.gmall.gmallpublisher.service.ProductStatsService;
import com.atguigu.gmall.gmallpublisher.service.impl.ProductStatsServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;


    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if(date==0){
            date=now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{   \"status\": 0,  \"data\":" + gmv + "}";

        System.out.println("11111111111111111111");
        return  json;
    }

    private int now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return   Integer.valueOf(yyyyMMdd);
    }

    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(@RequestParam(value = "date",defaultValue = "0") Integer date,
                                             @RequestParam(value = "limit",defaultValue = "5") Integer limit) {
        if(date==0){
            date=now();
        }

        Map map = productStatsService.getProductStatsByTrademark(date, limit);

        Set set = map.keySet();
        Collection values = map.values();

        return "{  " +
                "  \"status\": 0,  " +
                "  \"msg\": \"\",  " +
                "  \"data\": {  " +
                "    \"categories\": [  \"" +
                StringUtils.join(set,"\",\"") +
                "    \"],  " +
                "    \"series\": [  " +
                "      {  " +
                "        \"name\": \"手机品牌\",  " +
                "        \"data\": [  " +
                StringUtils.join(values,",")+
                "        ]  " +
                "      }  " +
                "    ]  " +
                "  }  " +
                "}";

    }

}
