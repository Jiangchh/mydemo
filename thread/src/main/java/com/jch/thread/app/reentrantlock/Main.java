package com.jch.thread.app.reentrantlock;

/**
 * @Author: jiangchenghua
 * @Date: 2021/1/19 10:39 上午
 */
//为了实现两个线程按顺序打印，12A34B56D
public class Main {


    static Integer[] createNumberArray(int maxNum) {
        Integer[] array = new Integer[maxNum];
        for (int i = 1; i < maxNum + 1; i++) {
            array[i - 1] = i;
        }
        return array;
    }

    static char[] createAlphaArray(char maxAlpha) {
        int length = (int) maxAlpha - (int) 'A' + 1;
        char[] array = new char[length];
        for (int i = 0; i < length; i++) {
            array[i] = (char) ((int) 'A' + i);
        }
        return array;
    }

    public static void main(String[] args) {
        Task task = new Task(createAlphaArray('Z'), createNumberArray(52));
        new Thread(new NumberThread(task)).start();
        new Thread(new AlphaThread(task)).start();
        System.err.println("getHoldCount:" + task.lock.getHoldCount());
        System.err.println("getQueueLength:" + task.lock.getQueueLength());
        System.err.println("getWaitQueueLength(alphaLock):" + task.lock.getWaitQueueLength(task.alphaLock));
        System.err.println("getWaitQueueLength(numLock):" + task.lock.getWaitQueueLength(task.numLock));
        System.err.println("isFair" + task.lock.isFair());
        System.err.println("isLocked:" + task.lock.isLocked());
        System.err.println("isHeldByCurrentThread" + task.lock.isHeldByCurrentThread());


    }
}
