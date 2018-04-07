package com.zuoqiang.test.transaction;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class Consumer {
    public Consumer() {
        try {
            String group_name = "transaction_consumer";
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
            consumer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
            consumer.subscribe("Transaction", "*");
            //consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new Listener());
            consumer.start();

        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Consumer c = new Consumer();
        System.out.println("Transaction Consumer start !");

    }

    class Listener implements MessageListenerConcurrently {
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            try {
                MessageExt msg = msgs.get(0);
                String topic = msg.getTopic();
                String msgBody = new String(msg.getBody(), "utf8");
                String tags = msg.getTags();
                System.out.println("收到消息: topic : " + topic + ", tags : " + tags + ",msg : " + msgBody);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }


}
