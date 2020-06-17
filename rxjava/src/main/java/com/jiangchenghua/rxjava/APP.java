package com.jiangchenghua.rxjava;

import io.reactivex.rxjava3.core.Flowable;

public class APP {
    public static void main(String[] args) {
        Flowable.fromArray(args).subscribe(s -> System.out.println("Hello " + s + "!"));
    }
}
