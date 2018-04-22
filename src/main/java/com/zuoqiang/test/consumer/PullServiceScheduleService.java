package com.zuoqiang.test.consumer;

import com.alibaba.rocketmq.client.consumer.*;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class PullServiceScheduleService {
    public static void main(String[] args) throws MQClientException {
        String group_name = "pull_consumer";
        final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(group_name);
        scheduleService.getDefaultMQPullConsumer().setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        scheduleService.setMessageModel(MessageModel.CLUSTERING);
        scheduleService.registerPullTaskCallback("TopicPull", new PullTaskCallback() {
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                MQPullConsumer consumer = context.getPullConsumer();
                try {
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if (offset < 0) {
                        offset = 0;
                    }
                    PullResult pullResult = consumer.pull(mq, "*", offset, 32);
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
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;

                    }
                    //存储offset，客户端每隔5s定时刷新到Broker
                    consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                    //设置过6000ms重新拉取数据,建议大于5s
                    context.setPullNextDelayTimeMillis(6000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        scheduleService.start();
    }
}
