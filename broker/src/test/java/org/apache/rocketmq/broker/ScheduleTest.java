package org.apache.rocketmq.broker;

import org.apache.rocketmq.common.ThreadFactoryImpl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduleTest {
    public static void main(String[] args) {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
                "BrokerControllerScheduledThread"));

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                System.out.println("this is a schedule");
            }
        },1000, 2000,TimeUnit.MILLISECONDS);
    }
}
