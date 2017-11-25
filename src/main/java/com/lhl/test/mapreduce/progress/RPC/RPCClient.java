package com.lhl.test.mapreduce.progress.RPC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by HL.L on 2017/11/25.
 */
public class RPCClient {

    public static void main(String[] args) throws IOException {
        IRPCInterface proxy = RPC.getProxy(IRPCInterface.class, IRPCInterface.versionID, new InetSocketAddress("localhost", 8888),
                new Configuration());

        String result = proxy.test("world");
        System.out.println("结果：" + result);
    }
}
