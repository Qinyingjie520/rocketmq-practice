package com.atguigu.rocketmq;

import com.atguigu.rocketmq.contant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 消息重复：
 * 我们设计一个去除重表，对消息的唯一key添加唯一索引
 * 每次消费消息的时候，先插入数据库，如果成功则执行业务逻辑
 * 如果不成功，则说明消息来过了，直接签收了
 *
 */

@SpringBootTest
public class RocketmqDemo2ApplicationTests {

    @Autowired
    private JdbcTemplate  jdbcTemplate;


    // 重复生产
    @Test
    public void repeatProducer() throws Exception {
        if(jdbcTemplate== null){
            throw new IllegalStateException("JdbcTemplate is null");
        }
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("repeat-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQProducer.start();
        String uuid = "d6c5483f-6499-4f93-8540-f80156c89c7a";

        System.out.println("uuid = "+uuid);
        Message m1 = new Message("repeatTopic",null,uuid,"扣减库存-1".getBytes());
        Message m2 = new Message("repeatTopic",null,uuid,"扣减库存-1".getBytes());
        Message m3 = new Message("repeatTopic",null,uuid,"扣减库存-1".getBytes());
        Message m4 = new Message("repeatTopic",null,uuid,"扣减库存-1".getBytes());
        defaultMQProducer.send(m1);
        defaultMQProducer.send(m2);
        defaultMQProducer.send(m3);
        defaultMQProducer.send(m4);
        System.out.println("发送消息成功！");
        // 关闭生产者
        defaultMQProducer.shutdown();
    }

    // 重复消费（不能）
    @Test
    public void repeatConsumer() throws Exception {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("repeat-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAMESERVER_ADDR);
        defaultMQPushConsumer.subscribe("repeatTopic","*");
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                // 先拿key
                String key = list.get(0).getKeys();
                System.out.println("拿到了key = "+key);
                // 插入数据库，因为我们在数据库对应的key是唯一的，所以重复消费会失败
                int update = jdbcTemplate.update("insert into order_oper_log(`type`, `order_sn`, `user`) values(1,?,123)", key);
                if(update == 1){
                    // 处理业务
                    System.out.println("消费了 ： "+new String(list.get(0).getBody()));
                    // 插入成功
                    System.out.println("插入成功");
                }else{
                    // 插入失败
                    System.out.println("插入失败");
                }



                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        defaultMQPushConsumer.start();
        System.out.println("消费者启动");
        System.in.read();

    }
}
