package com.zycao.pigTask;

import org.apache.pig.PigServer;
import org.apache.pig.ExecType;


public class JoinFirst1 {
    public static void main(String[] args) throws Exception {
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        String PATH = "/Users/czy/Desktop/czy/NEU/CS6240.tmp/HW_repo/HW3/HW3_FlightDelay/src/main/pig/JoinFirst_1.pig";
        pigServer.registerScript(PATH);
    }
}
