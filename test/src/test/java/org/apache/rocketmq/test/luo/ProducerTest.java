package org.apache.rocketmq.test.luo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.luo.base.BaseInfo;
import org.junit.Test;

import java.util.Date;
import java.util.List;

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
        producer.setInstanceName(producerInstance);
        producer.start();

        for (int i = 0; i < 10; i++) {
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
        producer.setInstanceName(producerInstance);
        producer.start();

        for (int i = 0; i < 1; i++) {
            Message message = new Message(topic, ("message " + i + new Date()).getBytes());
            message.setDelayTimeLevel(4);

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

            Thread.sleep(8000);
        }

        producer.shutdown();
    }

}
