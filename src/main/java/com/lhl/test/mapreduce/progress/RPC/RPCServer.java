package com.lhl.test.mapreduce.progress.RPC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by HL.L on 2017/11/25.
 */
public class RPCServer {

    public static void main(String[] args) throws IOException {
        RPC.Server server = new RPC.Builder(new Configuration())
               .setBindAddress("localhost")
                .setPort(8888)
                .setInstance(new RPCImpl())
                .setProtocol(IRPCInterface.class)
                .build();

        server.start();
    }


}
