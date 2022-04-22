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
package org.apache.rocketmq.example.quickstart;

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {

        /*
         * Instantiate with specified consumer group name.
         *
         * 1、DefaultMQPushConsumer：消息发送者将消息发送到Broker，然后Broker主动推送给订阅了该消息的消费者
         *  RocketMQ 推拉机制实现：严格意义上来讲，RocketMQ 并没有实现 PUSH 模式，而是对拉模式进行一层包装，在消费端开启一个线程 PullMessageService 循环向 Broke r拉取消息，一次拉取任务结束后马上又发起另一次拉取操作，实现准实时自动拉取
         *
         * 2、DefaultMQPushConsumer：消息发送者将消息发送到Broker，然后由消息消费者自发的向Broker拉取消息。
         *
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rmq");

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        //consumer.setNamesrvAddr("VM-24-9-centos:9876");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        /*
         * Specify where to start in case the specified consumer group is a brand new one.
         *
         * CONSUME_FROM_FIRST_OFFSET  从头开始消费消息
         * CONSUME_FROM_LAST_OFFSET 从上一次消费offset开始消费
         * CONSUME_FROM_TIMESTAMP 从某个时间点开始消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //消费最小线程、最大线程
        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(10);
        //超时时间
        consumer.setConsumeTimeout(1000);
        consumer.setConsumeMessageBatchMaxSize(10);
        /*
         * Subscribe one more more topics to consume.
         */
        consumer.subscribe("TopicTest", "*");
        //消息模式：集群，广播
        consumer.setMessageModel(MessageModel.CLUSTERING);

        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                /**
                 * [queueId=2, storeSize=180, queueOffset=486, sysFlag=0, bornTimestamp=1638243735920, bornHost=/10.0.24.9:50398,
                 * storeTimestamp=1638243735920, storeHost=/172.17.x.1:10911,
                 * msgId=AC11000100002A9F0000000000055604, commitLogOffset=349700,
                 * bodyCRC=1830206955, reconsumeTimes=0, preparedTransactionOffset=0, toString()=
                 * Message{topic='TopicTest', flag=0, properties={MIN_OFFSET=0, MAX_OFFSET=500, CONSUME_START_TIME=1638434532040, UNIQ_KEY=AC110001A16B3CD1A2F197DB5D7003B0, WAIT=true, TAGS=TagA},
                 * body=[72, 101, 108, 108, 111, 32, 82, 111, 99, 107, 101, 116, 77, 81, 32, 57, 52, 52], transactionId='null'}]
                 */
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                //return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
