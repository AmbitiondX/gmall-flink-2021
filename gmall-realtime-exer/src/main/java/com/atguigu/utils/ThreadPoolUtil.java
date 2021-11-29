package com.atguigu.utils;

import lombok.SneakyThrows;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor;

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>()
                    );
                }
            }
        }

        return threadPoolExecutor;
    }

    public static void main(String[] args) {

        ThreadPoolExecutor threadPoolExecutor = getThreadPoolExecutor();

        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.execute(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                    Thread.sleep(2000);
                }
            });
        }

    }
}
