package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor(){

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            60L,
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>()
                    );
                }
            }
        }
        return threadPoolExecutor;
    }
}
