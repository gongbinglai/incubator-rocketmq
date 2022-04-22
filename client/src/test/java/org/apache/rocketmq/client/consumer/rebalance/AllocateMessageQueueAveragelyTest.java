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

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class AllocateMessageQueueAveragelyTest {

    private String topic;
    private static final String CID_PREFIX = "CID-";

    @Before
    public void init() {
        topic = "topic_test";
    }

    public void printMessageQueue(List<MessageQueue> messageQueueList, String name) {
        if (messageQueueList == null || messageQueueList.size() < 1)
            return;
        System.out.println(name + ".......................................start");
        for (MessageQueue messageQueue : messageQueueList) {
            System.out.println(messageQueue);
        }
        System.out.println(name + ".......................................end");
    }

    @Test
    public void testAllocate1() {
        testAllocate(3, 2);
    }

    @Test
    public void testAllocate2() {
        testAllocate(10, 20);
    }

    @Test
    public void testRun100RandomCase() {
        for (int i = 0; i < 10; i++) {
            int consumerSize = new Random().nextInt(20) + 1;//1-20
            int queueSize = new Random().nextInt(20) + 1;//1-20
            testAllocate(queueSize, consumerSize);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }
    }

    public void testAllocate(int queueSize, int consumerSize) {
        AllocateMessageQueueStrategy allocateMessageQueueAveragely = new AllocateMessageQueueAveragely();

        List<MessageQueue> mqAll = createMessageQueueList(queueSize);
        List<String> cidAll = createConsumerIdList(consumerSize);

        List<MessageQueue> allocatedResAll = new ArrayList<MessageQueue>();

        Map<MessageQueue, String> allocateToAllOrigin = new TreeMap<MessageQueue, String>();

        List<String> cidBegin = new ArrayList<String>(cidAll);

        //System.out.println("cidAll:" + cidBegin.toString());
        for (String cid : cidBegin) {
            List<MessageQueue> rs = allocateMessageQueueAveragely.allocate("testConsumerGroup", cid, mqAll, cidBegin);
            for (MessageQueue mq : rs) {
                allocateToAllOrigin.put(mq, cid);
            }
        }

        for(MessageQueue mq:allocateToAllOrigin.keySet()){
            System.out.println(mq.getBrokerName()+"_"+mq.getQueueId()+",cid："+allocateToAllOrigin.get(mq));
        }


    }


    private List<String> createConsumerIdList(int size) {
        List<String> consumerIdList = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            consumerIdList.add(CID_PREFIX + String.valueOf(i));
        }
        return consumerIdList;
    }

    private List<MessageQueue> createMessageQueueList(int size) {
        List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>(size);
        for (int i = 0; i < size; i++) {
            MessageQueue mq = new MessageQueue(topic, "brokerName", i);
            messageQueueList.add(mq);
        }
        return messageQueueList;
    }
}
