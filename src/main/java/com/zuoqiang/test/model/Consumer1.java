package com.zuoqiang.test.model;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer1 {
    public Consumer1() throws MQClientException {
        String group_name = "message_consumer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
        consumer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        consumer.subscribe("Topic1", "Tag1||Tag2||Tag3");
        consumer.registerMessageListener(new Listener());
        consumer.start();
    }

    class Listener implements MessageListenerConcurrently {

        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            //先启动consumer,相当于每次消费一条记录
            MessageExt message = msgs.get(0);
            try {
                String topic = message.getTopic();
                String tag = message.getTags();
                String msgbody = new String(message.getBody(), "utf-8");
                System.out.println("收到消息" + "Topic:" + topic + ",Tag:" + tag + ",MsgBody:" + msgbody);
                //休眠一分钟表示业务处理失败
                Thread.sleep(60000);
            } catch (Exception e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    public static void main(String[] args) throws MQClientException {
        Consumer1 consumer1 = new Consumer1();
        System.out.println("C1启动！");
    }
}
