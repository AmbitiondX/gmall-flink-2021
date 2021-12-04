package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.gmallpublisher.mapper.ProductStatsMapper;
import com.atguigu.gmall.gmallpublisher.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProductStatsServiceImpl implements ProductStatsService {

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }

    @Override
    public Map getProductStatsByTrademark(int date, int limit) {

        List<Map> trademark = productStatsMapper.getProductStatsByTrademark(date, limit);
        HashMap<String, BigDecimal> hashMap = new HashMap<>();

        for (Map map : trademark) {
            hashMap.put((String)map.get("tm_name"), (BigDecimal) map.get("order_amount"));
        }

        return hashMap;
    }
}
