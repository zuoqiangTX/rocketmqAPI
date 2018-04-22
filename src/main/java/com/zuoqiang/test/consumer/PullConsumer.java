package com.zuoqiang.test.consumer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PullConsumer {
    //真实环境应该用DB记录位置
    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {
        String group_name = "pull_consumer";
        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(group_name);
        pullConsumer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        pullConsumer.start();

        //从topicTest中获取所有的队列（默认有四个队列）
        Set<MessageQueue> mgs = pullConsumer.fetchSubscribeMessageQueues("TopicPull");
        //遍历每一个队列，进行拉取数据
        for (MessageQueue mq : mgs) {
            System.out.println("consume from the queue" + mq);
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult pullResult = pullConsumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);

                    System.out.println(pullResult);
                    System.out.println(pullResult.getPullStatus());
                    //记录下次读取的的位置
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            List<MessageExt> list = pullResult.getMsgFoundList();
                            for (MessageExt ext : list) {
                                System.out.println(new String(ext.getBody()));
                            }
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            System.out.println("没有新数据");
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
        pullConsumer.shutdown();
    }

    private static void putMessageQueueOffset(MessageQueue queue, long offset) {
        offseTable.put(queue, offset);
    }

    private static long getMessageQueueOffset(MessageQueue queue) {
        Long offset = offseTable.get(queue);
        if (offset != null) {
            return offset;
        }
        return 0;
    }
}
