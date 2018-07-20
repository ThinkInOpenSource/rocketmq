package org.apache.rocketmq.test.luo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.test.luo.base.BaseInfo;
import org.junit.Test;

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
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setInstanceName(consumerInstance);
        consumer.subscribe(topic, "*");

        consumer.registerMessageListener((MessageListenerConcurrently) (msgList, context) -> {
            msgList.forEach(msg -> System.out.println(new String(msg.getBody())));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();

        // wait for consumer
        LockSupport.park();
    }

}
