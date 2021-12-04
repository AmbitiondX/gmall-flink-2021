package com.atguigu.gmall.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface ProductStatsMapper {

    @Select("select sum(order_amount) from product_stats_2021 where toYYYYMMDD(stt) = ${date}")
    BigDecimal getGMV(int date);
}
