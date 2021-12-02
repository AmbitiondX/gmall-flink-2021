package com.atguigu.gmall.gmallpublisher.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public interface ProductStatsService {

    BigDecimal getGMV(int date);

}
