package com.jch.thread.app.reentrantlock;

/**
 * @Author: jiangchenghua
 * @Date: 2021/1/19 10:34 上午
 */
//
public class AlphaThread implements Runnable {

    private Task task;

    public AlphaThread(Task task) {
        this.task = task;
    }

    @Override
    public void run() {
        task.printAlpha();
    }
}

