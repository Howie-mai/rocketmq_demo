package com.zhku.mh.rocketmq.cluster;


import com.zhku.mh.rocketmq.constants.Const;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ProducerCluster {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("test_cluster_producer_name");

        producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);

        producer.start();

        /*
         * topic: 主题
         * tag: 标签  消息过滤
         * key 用户自定义的key 唯一标识
         * content 消息内容实体（byte[]）
         */
        for (int i = 0; i < 1; i++) {
            /*
             * 创建消息
             */
            Message message = new Message("test_cluster_topic",
                    "Tag",
                    "key" + i,
                    ("Hello RocketMQ" + i).getBytes());

            /*
             * 发送消息
             */
            SendResult sendResult = producer.send(message);
            /*
             *  发出：SendResult [sendStatus=SEND_OK, msgId=AC151663198418B4AAC2738BC4E90000, offsetMsgId=AC15161A00002A9F0000000000000000, messageQueue=MessageQueue [topic=test_quick_topic, brokerName=broker-a, queueId=1], queueOffset=0]
             *  发出：SendResult [sendStatus=SEND_OK, msgId=AC151663198418B4AAC2738BC5A30001, offsetMsgId=AC15161A00002A9F00000000000000C1, messageQueue=MessageQueue [topic=test_quick_topic, brokerName=broker-a, queueId=2], queueOffset=0]
             *  发出：SendResult [sendStatus=SEND_OK, msgId=AC151663198418B4AAC2738BC5EC0002, offsetMsgId=AC15161A00002A9F0000000000000182, messageQueue=MessageQueue [topic=test_quick_topic, brokerName=broker-a, queueId=3], queueOffset=0]
             *  发出：SendResult [sendStatus=SEND_OK, msgId=AC151663198418B4AAC2738BC5F30003, offsetMsgId=AC15161A00002A9F0000000000000243, messageQueue=MessageQueue [topic=test_quick_topic, brokerName=broker-a, queueId=0], queueOffset=0]
             *  发出：SendResult [sendStatus=SEND_OK, msgId=AC151663198418B4AAC2738BC5F70004, offsetMsgId=AC15161A00002A9F0000000000000304, messageQueue=MessageQueue [topic=test_quick_topic, brokerName=broker-a, queueId=1], queueOffset=1]
             */
            System.out.println("发出：" + sendResult);
        }

        producer.shutdown();
    }
}
