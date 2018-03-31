package com.zuoqiang.test.model;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        String producer_group = "message_producer";
        DefaultMQProducer producer = new DefaultMQProducer(producer_group);
        producer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        producer.start();
        //发送一条数据
        for (int i = 1; i <= 1; i++) {
            Message message = new Message("Topic1", "Tag1", ("消息内容" + i).getBytes());
            try {
                SendResult sendResult = producer.send(message, 1000);
                System.out.println(sendResult);

            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);

            }
        }
        producer.shutdown();

    }
}
