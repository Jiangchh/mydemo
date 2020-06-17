package com.jiangchenghua.hystrix;


import com.google.common.util.concurrent.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.*;

public class APP {
    public static void main(String[] args)  {
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        final ListenableFuture<Integer> listenableFuture = executorService.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                System.out.println("call execute..");
                TimeUnit.SECONDS.sleep(1);
                return 7;
            }
        });
        Futures.addCallback(listenableFuture, new FutureCallback<Integer>() {

            @Override
            public void onSuccess(@Nullable Integer integer) {

            }

            @Override
            public void onFailure(Throwable throwable) {

            }
        },MoreExecutors.directExecutor());
    }
}
