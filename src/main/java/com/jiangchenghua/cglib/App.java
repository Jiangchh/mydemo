package com.jiangchenghua.cglib;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * Hello world!
 *
 */
public class App 
{
    public static int index=0;
    static class OOMObject{}
    public static void main(String[] args) {
      while(true){
          Enhancer enhancer =new Enhancer();
          enhancer.setSuperclass(OOMObject.class);
          enhancer.setUseCache(false);
          enhancer.setCallback(new MethodInterceptor(){
              @Override
              public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
                  return methodProxy.invokeSuper(objects,args);
              }
          });
          Object o = enhancer.create();
      }
    }


//    public static void main(String args[]){
//        Lock locka=new ReentrantLock();
//        Lock lockb=new ReentrantLock();
//        new Thread(()->{
//            try {
//                synchronized (locka){
//                    locka.wait(); // 只要不唤醒，这个线程就是一直死锁状态，当然这不是我们测试的方式，但是结果是一样的，范例一更好地表达了线程间的死锁
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//        }).start();
//        new Thread(()->{
//            try {
//                synchronized (lockb){
//                    lockb.wait();
//                    locka.wait();
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//        }).start();
//    }
    public static void printarray(int[] arr,int index){
        for (int i=0;i<arr.length;i++){
            System.err.print(arr[i]+",");
        }
        System.err.print("\n");
        App.index++;
    }
    /**
     * index 记录当前比较数字的下标,每个数字比较完+1
     * @param arr
     */
    public static void countSort(int[] arr) {
        int index=0;
        int swap=0;
        for (int i = 0; i <arr.length ; i++) {
         for(int j=i;j<=i&&j>=1;j--){
            if(arr[j-1]>arr[j]){
                swap=arr[j-1];
                arr[j-1]=arr[j];
                arr[j]=swap;
            }
             printarray(arr,index);
         }
        }
        System.err.println(App.index);
    }
}
