package com.zhku.mh.rocketmq.cluster;


import com.zhku.mh.rocketmq.constants.Const;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class ConsumerCluster {
    public static void main(String[] args) throws MQClientException {
        //订阅组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_cluster_consumer_name");

        consumer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);

        //从offset最后开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        // *: topic下所有的信息
        // tagA .....
        consumer.subscribe("test_cluster_topic", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                                                            ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                MessageExt me = list.get(0);
                try {
                    String topic = me.getTopic();
                    String tags = me.getTags();
                    String keys = me.getKeys();
//                    假装异常
//                    if(keys.equals("key1")){
//                        System.out.println("消息发送失败");
//                        int a = 1/0;
//                    }
                    String msg = new String(me.getBody(), RemotingHelper.DEFAULT_CHARSET);
                    System.out.println("topic：" + topic + "tag:" + tags + "key:" + keys + "msg:" + msg);
                } catch (Exception e) {
                    e.printStackTrace();
//                     重试次数
//                    int reconsumeTimes = me.getReconsumeTimes();
//                    System.out.println(reconsumeTimes);
//                    if(reconsumeTimes == 3){
//                        // 记录日志
//                        // 超过三次就不处理
//                        // 做补偿操作
//                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("consumer start....");
    }
}
