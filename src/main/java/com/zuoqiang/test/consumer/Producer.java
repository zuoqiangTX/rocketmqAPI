package com.zuoqiang.test.consumer;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("pull_producer");
        producer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message("TopicPull", "TagA", (" Hello, World! " + i).getBytes());
            SendResult sendResult = null;
            try {
                sendResult = producer.send(message);
                System.out.println(sendResult);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.shutdown();

    }
}