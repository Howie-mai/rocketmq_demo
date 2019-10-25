package com.zhku.mh.rocketmq.model;

import com.zhku.mh.rocketmq.constants.Const;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * ClassName：
 * Time：2019/10/24 14:30
 * Description：
 * Author： mh
 */
public class ProducerModel {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        String group_name = "test_model_producer_name";
        DefaultMQProducer producer = new DefaultMQProducer(group_name);
        producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);
        producer.start();

        for (int i = 0; i < 10; i++) {
            try {
                String tag = (i % 2 == 0) ? "TagA" : "TagB";
                Message msg = new Message("test_model_topic",// topic
                        tag,// tag
                        ("信息内容" + i).getBytes()// body
                );
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }
        producer.shutdown();
    }
}
