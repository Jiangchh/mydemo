package com.jch.thread.app.reentrantlock;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: jiangchenghua
 * @Date: 2021/1/19 4:44 下午
 */
public class Task {

    static int sum = 1;
    public ReentrantLock lock = new ReentrantLock();
    Condition alphaLock = lock.newCondition();
    Condition numLock = lock.newCondition();
    private char[] charArray;
    private Integer[] numArray;

    public Task(char[] charArray, Integer[] numArray) {
        this.charArray = charArray;
        this.numArray = numArray;
    }

    public void printAlpha() {
        lock.lock();
        for (int i = 0; i < charArray.length; i++) {
            try {
                if (sum < 3) {
                    alphaLock.await();
                }
                System.err.print(charArray[i]);
                numLock.signal();
                sum = 1;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lock.unlock();
    }

    public void printNum() {
        lock.lock();

        for (int i = 0; i < numArray.length; i++) {
            if (sum < 3) {
                System.err.print(numArray[i]);
                if (sum == 2) {
                    alphaLock.signal();
                }
            } else {
                try {
                    numLock.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.err.print(numArray[i]);
            }
            sum++;
        }
        lock.unlock();
    }
}
