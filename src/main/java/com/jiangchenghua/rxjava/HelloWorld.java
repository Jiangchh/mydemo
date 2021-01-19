package com.jiangchenghua.rxjava;

import io.reactivex.rxjava3.core.Flowable;

public class HelloWorld {
    public static void main(String[] args) {
        Flowable.just("Hello world").subscribe(System.out::println);
    }
}
