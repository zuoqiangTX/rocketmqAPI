package com.zuoqiang.test.transaction;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.*;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws MQClientException {
        String group_name = "transaction_producer";
        final TransactionMQProducer producer = new TransactionMQProducer(group_name);
        producer.setNamesrvAddr("10.211.55.9:9876;10.211.55.13:9876");
        //设置回查函数并发数、队列数
        producer.setCheckThreadPoolMaxSize(20);
        producer.setCheckThreadPoolMinSize(5);
        producer.setCheckRequestHoldMax(2000);
        //使用之前调用start
        producer.start();

        //服务器回调producer，检查本地事务分支成功还是失败（当MQ没收到本地事务执行完后【也就是第二次】确认消息,定时回调）
        producer.setTransactionCheckListener(new TransactionCheckListener() {
            public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
                //msg.key 查询数据库看是否有的
                System.out.println("状态 -- " + new String(msg.getBody()));
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        //本地事务实现类
        TransactionExecutorImpl transactionExecutor = new TransactionExecutorImpl();
        for (int i = 1; i <= 1; i++) {
            try {
                Message msg = new Message("Transaction", "Transaction" + i, "key", ("Hello RocketMQ" + i).getBytes());
                SendResult sendResult = producer.sendMessageInTransaction(msg, transactionExecutor, "tq");
                System.out.println(sendResult);
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //应用退出的时候，调用shutdown来清理资源，关闭连接，建议应用在Tomcat的退出钩子中调用shutdown方法
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                producer.shutdown();
            }
        }));
        System.exit(0);

    }
}
