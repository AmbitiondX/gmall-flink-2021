package com.atguigu.func;

import com.atguigu.bean.Bean3;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class CustomerWM implements WatermarkGenerator<Bean3> {
    //最大时间戳
    private Long maxTs;

    //最大延迟时间
    private Long maxDelay;

    //构造方法
    public CustomerWM(Long maxDelay) {
        this.maxDelay = maxDelay;
        this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
    }

    //当数据来的时候调用
    @Override
    public void onEvent(Bean3 event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(eventTimestamp, maxTs);

    }

    //周期性调用
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

        System.out.println("生成Watermark");

        output.emitWatermark(new Watermark(maxTs-maxDelay-1L));

    }

}
