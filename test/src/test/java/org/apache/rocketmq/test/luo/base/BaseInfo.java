package org.apache.rocketmq.test.luo.base;

/**
 * Description: 公共信息
 *
 * @author xiangnan
 * date 2018/7/21 0:06
 */
public interface BaseInfo {
    String namesrvAddr = "192.168.85.130:9876";

    String producerGroup = "producerGroup";
    String producerInstance = "producerInstance";

    String consumerGroup = "consumerGroup";
    String consumerInstance = "consumerInstance";

    String topic = "test-topic";
}
