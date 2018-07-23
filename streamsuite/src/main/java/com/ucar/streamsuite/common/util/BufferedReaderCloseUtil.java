package com.ucar.streamsuite.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.concurrent.*;

public class BufferedReaderCloseUtil {

    //设置的大一点防止，提交人过多任务加不进去的情况
    private final static ExecutorService bufferedReaderCloseService = Executors.newFixedThreadPool(20);

    public static final Logger LOGGER = LoggerFactory.getLogger(BufferedReaderCloseUtil.class);

    public static void pendingClose(BufferedReader bufferedReader,Integer latestClose,String memo){
        if(bufferedReader ==  null || latestClose == null){
            return;
        }
        bufferedReaderCloseService.submit(new BufferedReaderClose(bufferedReader,latestClose, memo));
    }

    private static class BufferedReaderClose implements Runnable {
        private BufferedReader bufferedReader;
        private Integer planCloseSec;
        private String memo;

        public BufferedReaderClose(BufferedReader bufferedReader,Integer planCloseSec,String memo) {
            this.bufferedReader = bufferedReader;
            this.planCloseSec = planCloseSec;
            this.memo = memo;
        }

        public void run()  {
            try {
                long begin = System.currentTimeMillis();
                while((((System.currentTimeMillis() - begin) /1000) < planCloseSec.longValue()) && bufferedReader != null){
                    TimeUnit.SECONDS.sleep(1);
                }
                LOGGER.error("InputStreamReaderClose is run :" + memo);
            }catch (Throwable e){
            }finally {
                bufferedReader = null;
            }
        }
    }

}
