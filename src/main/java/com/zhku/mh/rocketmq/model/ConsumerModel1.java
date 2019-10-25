package com.zhku.mh.rocketmq.model;

import com.zhku.mh.rocketmq.constants.Const;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * ClassName：
 * Time：2019/10/24 14:31
 * Description：
 * Author： mh
 */
public class ConsumerModel1 {
    public ConsumerModel1() {
        try {
            String group_name = "test_model_consumer_name";
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
            consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
            consumer.subscribe("test_model_topic", "*");
            consumer.setMessageModel(MessageModel.CLUSTERING);
//            consumer.setMessageModel(MessageModel.BROADCASTING);
            consumer.registerMessageListener(new Listener());
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class Listener implements MessageListenerConcurrently {
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            try {
                for (MessageExt msg : msgs) {
                    String topic = msg.getTopic();
                    String msgBody = new String(msg.getBody(), "utf-8");
                    String tags = msg.getTags();
                    //if(tags.equals("TagA")) {
                    System.out.println("收到消息：" + "  topic :" + topic + "  ,tags : " + tags + " ,msg : " + msgBody);
                    //}
                }
            } catch (Exception e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

    }

    public static void main(String[] args) {
        ConsumerModel1 c1 = new ConsumerModel1();
        System.out.println("c1 start..");

    }
}
