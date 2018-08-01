package org.apache.rocketmq.test.luo;

import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.luo.base.BaseInfo;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Description: 生产者测试用例
 *
 * @author xiangnan
 * date 2018/7/21 0:10
 */
public class ProducerTest implements BaseInfo {

    /**
     * 默认生产者示例
     */
    @Test
    public void defaultProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrvAddr);
        producer.start();

        for (int i = 0; i < 1; i++) {
            Message message = new Message(topic, ("message " + i).getBytes());
            SendResult result = producer.send(message);
            System.out.println(result);
        }

        producer.shutdown();
    }

    /**
     * 延时消息生产者
     *
     * messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
     * 延时等级从1开始，为0表示不延时
     */
    @Test
    public void delayMessageProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrvAddr);
        producer.start();

        for (int i = 1; i <= 18; i++) {
            Message message = new Message(topic, ("message " + i + new Date()).getBytes());
            message.setDelayTimeLevel(i);

            SendResult result = producer.send(message);
            System.out.println(result);
        }

        producer.shutdown();
    }

    /**
     * 顺序消息生产者
     */
    @Test
    public void orderMessageProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName(producerInstance);
        producer.start();

        for (int i = 0; i < 1; i++) {
            Message message = new Message(topic, ("message " + i).getBytes());
            message.setDelayTimeLevel(4);

            SendResult result = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get((Integer) arg % mqs.size());
                }
            }, 0);
            System.out.println(result);
        }

        producer.shutdown();
    }

    /**
     * 事务消息发送示例
     */
    @Test
    public void transactionProducer() throws Exception {
        String groupName = "transaction-group";
        TransactionMQProducer producer = new TransactionMQProducer(groupName);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setTransactionCheckListener(new TransactionCheckListener() {
            // broker回查事务消息状态的回调
            @Override
            public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
                System.out.println("checkLocalTransactionState: " + msg);
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        producer.start();

        for (int i = 0; i < 1; i++) {
            System.out.println("send message " + i + " " + new Date());
            Message message = new Message(topic, ("message " + i + " " + new Date()).getBytes());

            TransactionSendResult result = producer.sendMessageInTransaction(message, new LocalTransactionExecuter() {
                @Override
                public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
                    System.out.println("executeLocalTransactionBranch: " + msg);
                    System.out.println("executeLocalTransactionBranch: " + arg);
                    // 本地事务操作

                    return LocalTransactionState.COMMIT_MESSAGE;
                }
            }, null);

            System.out.println(result);
        }

//        LockSupport.park();
        producer.shutdown();
    }

}
