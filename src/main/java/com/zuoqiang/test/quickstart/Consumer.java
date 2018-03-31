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
                System.out.println("消息条数为：" + msgs.size());
                //先启动consumer,每次消费数量为1,原子消费。
                MessageExt message = msgs.get(0);
                try {
                    String topic = message.getTopic();
                    String msgBody = new String(message.getBody(), "utf-8");
                    String tag = message.getTags();
                    System.out.println("收到消息: " + "Topic" + topic + ",Tags " + tag + ", msg : " + msgBody);

                    //一定要注意先启动Consumer订阅消息，然后再启动Producer发送消息。

                    //失败一次看看，记得要先启动消费者，保证每次消费单条消息
                        /*if (" Hello, World! 4".equals(msgBody)) {
                            System.out.println("============消息处理失败开始============");
                            System.out.println(message );
                            System.out.println(msgBody);
                            System.out.println("============消息处理失败结束============");
                            int n = 1 / 0;
                        }*/
                } catch (Exception e) {
                    e.printStackTrace();
                    if (message.getReconsumeTimes() == 2) {
                        //记录日志 ，操作等等
                        System.out.println("重试2次，还是没有成功，记录相关日志，进行处理");
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("Consumer start");

    }
}
