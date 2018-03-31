package com.zuoqiang.test.quickstart;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class Consumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quick_consumer");
        consumer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");

        /*设置第一次启动从队列头部还是尾部消费
        如果是非第一次，按照上次消费的位置消费
        */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicQuickStart", "*");
        //设置批量消费数量，只在Producer先启动的情况下生效。
        consumer.setConsumeMessageBatchMaxSize(10);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                //System.out.println(Thread.currentThread().getName() + "Receive New Messages :" + msgs);
                try {
                    System.out.println("消息条数为：" + msgs.size());
                    //先启动consumer,每次消费数量为1。
                    //Message msg = msgs.get(0);
                    for (Message msg : msgs) {
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(), "utf-8");
                        String tag = msg.getTags();
                        System.out.println("收到消息: " + "Topic" + topic + ",Tags " + tag + ", msg : " + msgBody);
                    }
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    //失败了稍后再次发送
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consumer start");

    }
}
