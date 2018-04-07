package com.zuoqiang.test.order;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Consumer {
    public Consumer() throws Exception{
        String group_name = "order_consumer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
        consumer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTest", "*");
        consumer.registerMessageListener(new Linstener());
        consumer.start();
        System.out.println("Consumer start !");
    }

    public static void main(String[] args) throws Exception{
        Consumer c = new Consumer();

    }

    class Linstener implements MessageListenerOrderly {
        private Random random = new Random();

        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {

            context.setAutoCommit(true);
            for (MessageExt msg : msgs) {
                System.out.println(msg + ",context:" + new String(msg.getBody()));
            }
            try {
                //模拟业务处理
                TimeUnit.SECONDS.sleep(random.nextInt(5));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return ConsumeOrderlyStatus.SUCCESS;
        }

    }


}
