package com.jiangchenghua.hystrix;


import com.google.common.util.concurrent.*;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class MultiFunctionTest{
    //定义一个线程池，用于处理所有任务 -- MoreExecutors
    private final static ListeningExecutorService sService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());


    /**
     * Futures.transformAsync()支持多个任务链式异步执行，并且后面一个任务可以拿到前面一个任务的结果
     */

    public static void multiTaskTransformAsyncTest() {

        final CountDownLatch latch = new CountDownLatch(1);

        // 第一个任务
        ListenableFuture<String> task1 = sService.submit(() -> {
            System.out.println("第一个任务开始执行...");
            try {
                Thread.sleep(10 * 1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return "第一个任务的结果";
        });

        // 第二个任务，里面还获取到了第一个任务的结果
        AsyncFunction<String, String> queryFunction = new AsyncFunction<String, String>() {
            public ListenableFuture<String> apply(String input) {
                return sService.submit(new Callable<String>() {
                    public String call() throws Exception {
                        System.out.println("第二个任务开始执行...");
                        try {
                            Thread.sleep(10 * 1000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return input + " & 第二个任务的结果 ";
                    }
                });
            }
        };

        // 把第一个任务和第二个任务关联起来
        ListenableFuture<String> first = Futures.transformAsync(task1, queryFunction, sService);

        // 监听返回结果
        Futures.addCallback(first, new FutureCallback<String>() {
            public void onSuccess(String result) {
                System.out.println("结果: " + result);
                latch.countDown();
            }

            public void onFailure(Throwable t) {
                System.out.println(t.getMessage());
                latch.countDown();
            }
        }, MoreExecutors.directExecutor());

        try {
            // 等待所有的线程执行完
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        multiTaskTransformAsyncTest();
    }

}