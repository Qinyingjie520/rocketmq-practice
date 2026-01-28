package com.atguigu.rocketmq.demo;

import com.atguigu.rocketmq.contant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.List;

/**
 *
 * 重试的时间间隔：
 * 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
 *
 * 默认重试16次
 * 1.如果重试了16次都失败了？
 * 2.能否自定义重试次数？
 * 3.当消息消费失败的时候，该如何正确的处理？
 *
 *
 */


// 消息重试机制
public class RetryTestDemo {

    /**
     * 生产者发送消息一般不会重试，MQ服务器不存在，重试也没有用
     *
     * @throws Exception
     */
    @Test
    public void retryProducer() throws Exception {
        // 创建一个生产者
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("retry-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        // 设置重试次数
        defaultMQProducer.setRetryTimesWhenSendFailed(3);
        // 设置异步重试次数
        defaultMQProducer.setRetryTimesWhenSendAsyncFailed(3);
        // 启动
        defaultMQProducer.start();
        // 发送消息
        Message message = new Message("retryTopic","我是消息，我是消息vipipipip1-dsadsadasda".getBytes());
        // 发送
        defaultMQProducer.send(message);
        //
        System.out.println("发送完成");
        // 关闭
        defaultMQProducer.shutdown();
    }
    // 消费者-重试机制
    @Test
    public void retryConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        consumer.subscribe("retryTopic","*");
        // 设置重试次数
        consumer.setMaxReconsumeTimes(5);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.println("当前时间："+sdf.format(System.currentTimeMillis()));
                System.out.println(list.get(0).getReconsumeTimes());
                System.out.println("我消费了消息： "+new String(list.get(0).getBody()));
                // 业务出错了 返回null ,返回RECONSUME_LATER都会重试

                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        consumer.start();
        System.out.println("消费者启动");
        System.in.read();

    }
}
