package com.jiangchenghua.cglib;

public class Demo {
    public static Demo SAVE_HOOK=null;
    public void isAlive(){
        System.out.println("yes, i am still alive");
    }
    public Demo(){
        System.out.println("");
    }
    @Override
    protected void finalize() throws Throwable {

        System.out.println("finalize method executed!");
        super.finalize();
        Demo.SAVE_HOOK=this;
    }
    private static final int _1MB=1024*1024;
    public static void main(String[] args) {
        byte[]allocation;
        allocation=new byte[4*_1MB];
    }
    public void oldFunction(){

    }
}
