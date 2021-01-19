package com.jch.thread.app.wait_notify;

/**
 * @Author: jiangchenghua
 * @Date: 2021/1/19 4:44 下午
 */
public class Task {

    static int sum = 1;
    private char[] charArray;
    private Integer[] numArray;

    public Task(char[] charArray, Integer[] numArray) {
        this.charArray = charArray;
        this.numArray = numArray;
    }

    public void printAlpha() {
        for (int i = 0; i < charArray.length; i++) {
            try {
                if (sum < 3) {
                    synchronized (charArray) {
                        charArray.wait();
                    }
                }
                System.err.print(charArray[i]);
                synchronized (numArray) {
                    numArray.notifyAll();
                    sum = 1;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void printNum() {
        for (int i = 0; i < numArray.length; i++) {
            if (sum < 3) {
                System.err.print(numArray[i]);
                if (sum == 2) {
                    synchronized (charArray) {
                        charArray.notifyAll();
                    }
                }
            } else {

                synchronized (numArray) {
                    try {
                        numArray.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.err.print(numArray[i]);
            }
            sum++;
        }
    }
}
