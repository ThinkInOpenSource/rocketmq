package org.apache.rocketmq.test.luo;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueByMachineRoom;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.test.luo.base.BaseInfo;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Description: 消费者测试用例
 *
 * @author xiangnan
 * date 2018/7/20 23:55
 */
public class ConsumerTest implements BaseInfo {

    /**
     * 默认消费者示例
     *
     * 集群消费模式
     */
    @Test
    public void defaultConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup + "4");
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.subscribe(topic, "*");

        // 默认ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        consumer.registerMessageListener((MessageListenerConcurrently) (msgList, context) -> {
            msgList.forEach(msg -> {
                System.out.println(formatDate(null) + Thread.currentThread().getName() + ": " + new String(msg.getBody()));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();

        // wait for consumer
        LockSupport.park();
    }

    /**
     * 广播消费模式
     */
    @Test
    public void broadcastConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setInstanceName(consumerInstance);
        consumer.subscribe(topic, "*");
        consumer.setMessageModel(MessageModel.BROADCASTING);

        consumer.registerMessageListener((MessageListenerConcurrently) (msgList, context) -> {
            msgList.forEach(msg -> {
                System.out.println(formatDate(null) + Thread.currentThread().getName() + ": " + new String(msg.getBody()));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();

        // wait for consumer
        LockSupport.park();
    }

    /**
     * 顺序消费模式
     */
    @Test
    public void orderConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setInstanceName(consumerInstance);
        consumer.subscribe(topic, "*");

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgList, ConsumeOrderlyContext context) {
                msgList.forEach(msg -> {
                    System.out.println(formatDate(null) + Thread.currentThread().getName() + ": " + new String(msg.getBody()));
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
        LockSupport.park();
    }

    private String formatDate(Date date) {
        return "[" + DateFormatUtils.format(date == null ? new Date() : date, "yyyy-MM-dd hh:mm:ss") + "] ";
    }

}
