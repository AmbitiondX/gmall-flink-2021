package com.atguigu.gmall.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ProductStatsMapper {

    //获取商品交易额
    @Select("select sum(order_amount) order_amount  " +
            "from product_stats_2021 where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);

    /*
    获取某一天不同品牌的交易额
    如果mybatis的方法中，有多个参数，每个参数前需要用@Param注解指定参数的名称*/
    @Select("select tm_name,sum(order_amount)order_amount from product_stats_2021 where toYYYYMMDD(stt) = ${date} group by tm_name order by order_amount desc limit ${limit};")
    List<Map> getProductStatsByTrademark(@Param("date") int date, @Param("limit") int limit);


}
