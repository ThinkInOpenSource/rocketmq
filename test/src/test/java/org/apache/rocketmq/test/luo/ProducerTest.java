package org.apache.rocketmq.test.luo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.test.luo.base.BaseInfo;
import org.junit.Test;

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

}
