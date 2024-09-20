/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }

        //所有的主题队列
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }

        //所有客户端Id
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        //计算当前客户端在所有客户端列表的顺序，全局一致。保证了所有客户端计算一致
        int index = cidAll.indexOf(currentCID);

        //
        int mod = mqAll.size() % cidAll.size();

        //均摊到每个消费端，有多少个
        int averageSize =
            //如果客户端比队列多，则每个客户端分配一个队列。否则通过整除来分配
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());

        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;

        //需要分配的次数
        int range = Math.min(averageSize, mqAll.size() - startIndex);

        //挑选要分配给自己的
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }

    public static void main(String[] args) {
        AllocateMessageQueueAveragely allocateMessageQueueAveragely = new AllocateMessageQueueAveragely();
        String currentCID = "1";
//        List<String> cidAll = Arrays.asList("1","2","3","4","5","6","7","8","9");
        List<String> cidAll = Arrays.asList("1","2","3","4","5");
        List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
        mqAll.add(new MessageQueue("test","broker-a",0));
        mqAll.add(new MessageQueue("test","broker-a",1));
        mqAll.add(new MessageQueue("test","broker-a",2));
        mqAll.add(new MessageQueue("test","broker-a",3));
        mqAll.add(new MessageQueue("test","broker-a",4));
        mqAll.add(new MessageQueue("test","broker-a",5));
        mqAll.add(new MessageQueue("test","broker-a",6));
        mqAll.add(new MessageQueue("test","broker-a",7));
        mqAll.add(new MessageQueue("test","broker-a",8));
        mqAll.add(new MessageQueue("test","broker-a",9));
        mqAll.add(new MessageQueue("test","broker-a",10));
        mqAll.add(new MessageQueue("test","broker-a",11));

        List<MessageQueue> test = allocateMessageQueueAveragely.allocate("test", currentCID, mqAll, cidAll);

        System.out.println(test);
    }
}
