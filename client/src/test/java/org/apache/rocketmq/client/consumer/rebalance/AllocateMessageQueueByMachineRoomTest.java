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

public class AllocateMessageQueueByMachineRoomTest {

    private String topic;
    private static final String CID_PREFIX = "CID-";

    @Before
    public void init() {
        topic = "topic_test";
    }

    @Test
    public void testAllocate1() {
        testAllocate(4, 4);
    }

    public void testAllocate(int queueSize, int consumerSize) {

        List<MessageQueue> mqAll = createMessageQueueList(queueSize);
        List<String> cidAll = createConsumerIdList(consumerSize);

        List<MessageQueue> allocatedResAll = new ArrayList<MessageQueue>();

        Map<MessageQueue, String> allocateToAllOrigin = new TreeMap<MessageQueue, String>();

        List<String> cidBegin = new ArrayList<String>(cidAll);


        Map<String,Set<String>> consumerIDCsMap = new HashMap<String,Set<String>>();
        consumerIDCsMap.put("CID-0",new HashSet<String>(Arrays.asList("machine_room0","machine_room1")));
        consumerIDCsMap.put("CID-1",new HashSet<String>(Arrays.asList("machine_room1")));
        consumerIDCsMap.put("CID-2",new HashSet<String>(Arrays.asList("machine_room2")));
        consumerIDCsMap.put("CID-3",new HashSet<String>(Arrays.asList("machine_room2")));


        //System.out.println("cidAll:" + cidBegin.toString());
        int machineRoomIndex=0;
        for (String cid : cidBegin) {
            AllocateMessageQueueByMachineRoom allocateMessageQueueAveragely = new AllocateMessageQueueByMachineRoom();
            //指定机房名称  machine_room1、machine_room2
//            String machineRoom = "machine_room"+machineRoomIndex;
//            Set<String> consumerIDCs = new HashSet<String>();
//            consumerIDCs.add(machineRoom);
            allocateMessageQueueAveragely.setConsumeridcs(consumerIDCsMap.get(cid));
            List<MessageQueue> rs = allocateMessageQueueAveragely.allocate("testConsumerGroup", cid, mqAll, cidBegin);
            machineRoomIndex++;
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
        for(int roomIndex=0;roomIndex<3;roomIndex++){
            for(int i=0;i<4;i++){
                MessageQueue mq = new MessageQueue(topic, "machine_room"+roomIndex+"@brokerName", i);
                messageQueueList.add(mq);
            }
        }
        return messageQueueList;
    }
}
