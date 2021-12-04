package com.atguigu.gmall.gmallpublisher.service;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
public interface ProductStatsService {

    BigDecimal getGMV(int date);

    Map getProductStatsByTrademark(int date, int limit);
}
