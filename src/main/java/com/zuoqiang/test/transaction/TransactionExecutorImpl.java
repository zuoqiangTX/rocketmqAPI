package com.zuoqiang.test.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.message.Message;

public class TransactionExecutorImpl implements LocalTransactionExecuter {
    public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
        System.out.println("" + new String(msg.getBody()));
        System.out.println("arg = " + arg);
        String tag = msg.getTags();
        if (("Transaction1").equals(tag)) {
            //这里有个分阶段提交任务的概念
            System.out.println("这里处理业务逻辑，操作数据库，失败情况下执行RollBack");
            //执行完事务 发送确认状态给MQ
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
//        return LocalTransactionState.ROLLBACK_MESSAGE;
//        return LocalTransactionState.UNKNOW;
    }

}
