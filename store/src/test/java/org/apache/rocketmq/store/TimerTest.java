package org.apache.rocketmq.store;


import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.junit.Test;

import java.util.Timer;
import java.util.TimerTask;

public class TimerTest {

    @Test
    public void testTimer() throws InterruptedException {
        Timer timer = new Timer("ScheduleMessageTimerThread", true);

        timer.schedule(new WorkTask(), 1000L);

        Thread.sleep(10000000000000L);
    }

    class WorkTask extends TimerTask{

        @Override
        public void run() {
            System.out.println("this is a timer test");
        }
    }

}
