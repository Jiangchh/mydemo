package com.jiangchenghua.cglib;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Consumer {

    public static void main(String[] as) {

        TestService service = (TestService) Proxy.newProxyInstance(TestService.class.getClassLoader(), new Class<?>[]{TestService.class}, (Object proxy, Method method, Object[] args)
        -> {
            try(Socket socket = new Socket()){
                socket.connect(new InetSocketAddress(12306));
                ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
                os.writeUTF(method.getName());
                os.writeObject(method.getParameterTypes());
                os.writeObject(args);
                return new ObjectInputStream(socket.getInputStream()).readObject();
            }catch (Exception e){
                return null;
            }
        });
        System.out.println(service.print("abc"));
    }
}
