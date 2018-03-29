package com.zuoqiang.test.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("quick_producer");
        producer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        producer.start();

        for (int i = 0; i < 100; i++) {
            Message message = new Message("TopicQuickStart", "TagA", (" Hello, World! " + i).getBytes());
            SendResult sendResult = null;
            try {
                sendResult = producer.send(message);
                System.out.println(sendResult);

            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);

            }
        }
        producer.shutdown();
    }
}