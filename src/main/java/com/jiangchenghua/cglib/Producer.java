package com.jiangchenghua.cglib;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Producer {

    public static void main(String[] args)  {

        TestService service = new TestServiceImpl();

        try (ServerSocket serverSocket = new ServerSocket()){
            serverSocket.bind(new InetSocketAddress(12306));

            try(Socket accept = serverSocket.accept()){
                ObjectInputStream is = new ObjectInputStream(accept.getInputStream());
                String methodName = is.readUTF();
                Class<?>[] parameterTypes = (Class<?>[]) is.readObject();
                Object[] arguments = (Object[]) is.readObject();
                Object result = TestService.class.getMethod(methodName,parameterTypes).invoke(service,arguments);
                new ObjectOutputStream(accept.getOutputStream()).writeObject(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}