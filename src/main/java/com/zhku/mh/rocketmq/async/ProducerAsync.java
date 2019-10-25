package com.zhku.mh.rocketmq.async;


import com.zhku.mh.rocketmq.constants.Const;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/*
* 同步集群发送信息
*/
public class ProducerAsync {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("test_async_producer_name");

        producer.setNamesrvAddr(Const.NAMESRV_ADDR_MASTER_SLAVE);

        producer.start();

        /*
         * topic: 主题
         * tag: 标签  消息过滤
         * key 用户自定义的key 唯一标识
         * content 消息内容实体（byte[]）
         */
        for (int i = 0; i < 1; i++) {
            Message message = new Message("test_async_topic",
                    "Tag",
                    "key" + i,
                    ("Hello RocketMQ" + i).getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    throwable.printStackTrace();
                    System.err.println("----发送失败");
                }
            });
        }

//        producer.shutdown();
    }
}
