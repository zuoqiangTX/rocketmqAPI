package com.zuoqiang.test.order;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Producer {
    public static void main(final String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        String group_name = "order_producer";
        DefaultMQProducer producer = new DefaultMQProducer(group_name);
        producer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        producer.start();

        String[] tags = new String[]{"TagA", "TagB", "TagC"};
        Date date = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd: HH:mm:ss");
        String dateStr = format.format(date);

        for (int i = 1; i <= 10; i++) {
            String body = dateStr + "Hello RocketMQ" + i;
            Message msg = new Message("TopicTest", tags[i % tags.length], "KEY" + i, body.getBytes());
            //顺序消费，保证消息进入同一个队列中去。
            SendResult result = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    return mqs.get(id);
                }
            }, 0);//0是队列的下标
            System.out.println("SendResult is : " + "body" + body);
        }

        producer.shutdown();
    }
}