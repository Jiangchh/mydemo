package com.jiangchenghua.cglib;

public class TestServiceImpl implements TestService{

    @Override
    public String print(String s) {
        return "**"+s+"**";
    }
}