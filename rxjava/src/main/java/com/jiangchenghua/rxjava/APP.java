package com.jiangchenghua.rxjava;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.parallel.ParallelFlowable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class APP {
    public static void main(String[] args) {
        ParallelFlowable<Integer> source = Flowable.range(1, 1000).parallel();
        ParallelFlowable<Integer> psource = source.runOn(Schedulers.io());
        Flowable<Integer> result = psource.filter(v -> v % 3 == 0).map(v -> v * v).sequential();
    }
}
