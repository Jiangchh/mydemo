package com.jch.thread.app.wait_notify;

/**
 * @Author: jiangchenghua
 * @Date: 2021/1/19 10:28 上午
 */

public class NumberThread implements Runnable {

    private Task task;

    public NumberThread(Task task) {
        this.task = task;
    }

    @Override
    public void run() {
        task.printNum();
    }
}



