package com.jiangchenghua.hystrix;

import rx.Observable;
import rx.Observer;
import rx.functions.Action1;

import java.util.concurrent.Future;

public class APP {
    public static void main(String[] args) {
//        Observable<String> world = new CommandHelloWorld("World").observe();
//        world.subscribe(new Action1<String>() {
//            @Override
//            public void call(String s) {
//                System.err.println("此处是subscribe:"+s);
//            }
//        });
        Observable<String> fWorld = new CommandHelloWorld("World").observe();
        Observable<String> fBob = new CommandHelloWorld("Bob").observe();
        // non-blocking
        // - this is a verbose anonymous inner-class approach and doesn't do assertions
        fWorld.subscribe(new Observer<String>() {
            @Override
            public void onCompleted() {
                // nothing needed here
            }
            @Override
            public void onError(Throwable e) {
                e.printStackTrace();
            }
            @Override
            public void onNext(String v) {
                System.out.println("onNext: " + v);
            }
        });
        // non-blocking
        // - also verbose anonymous inner-class
        // - ignore errors and onCompleted signal
        fBob.subscribe(new Action1<String>() {
            @Override
            public void call(String v) {
                System.out.println("onNext: " + v);
            }

        });
    }
}
