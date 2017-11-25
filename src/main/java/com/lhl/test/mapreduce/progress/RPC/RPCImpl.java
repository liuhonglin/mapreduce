package com.lhl.test.mapreduce.progress.RPC;

/**
 * Created by HL.L on 2017/11/25.
 */
public class RPCImpl implements IRPCInterface {

    public String test(String str) {
        System.out.println("server invoke, arg = " + str);
        return "hello " + str;
    }
}
