package com.atguigu.rocketmqdemo2;

import com.alibaba.fastjson.JSON;
import com.atguigu.rocketmqdemo2.domain.MsgModel;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Arrays;
import java.util.List;

@SpringBootTest
class RocketmqDemo2ApplicationTests {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    // 发送消息
    @Test
    void contextLoads() {

        // 发送同步消息
        rocketMQTemplate.syncSend("bootTestTopic","我是boot的一个同步消息");

        // 发送异步消息
        rocketMQTemplate.asyncSend("bootTestTopic", "我是boot的异步的消息", new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("成功");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("失败");
            }
        });

//        发送单向消息
        rocketMQTemplate.sendOneWay("bootTestTopic","我是boot的一个单向消息");


        Message<String> msg= MessageBuilder.withPayload("我是boot的一个延迟--消息").build();
        // 发送延时消息
        rocketMQTemplate.syncSend("bootTestTopic", msg , 3000,3);



        // 发送顺序消息 需要将一组消息都发送到同一个队列中，消费者需要单线程去消费
       List<MsgModel> msgModelList = Arrays.asList(
               new MsgModel("qwer",1,"下单"),
               new MsgModel("qwer",1,"支付"),
                new MsgModel("qwer",1,"物流"),
                new MsgModel("zxcv",2,"下单"),
                new MsgModel("zxcv",2,"支付"),
                new MsgModel("zxcv",2,"物流")
       );
       msgModelList.forEach(msgModel -> {
           // 发送
           rocketMQTemplate.syncSendOrderly("orderlyTopic", JSON.toJSONString(msgModel), msgModel.getOrderSn());
       });



    }
//    测试消息的消费模式：集群模式 广播模式
    @Test
    public  void testSendRocketMQMessage(){
        for (int i = 1; i <11; i++) {
            rocketMQTemplate.syncSend("modeTopic", "我是第"+i+"个消息");
            System.out.println("发送成功");
        }


    }
//    测试消息轨迹发送消息
    @Test
    public void testSendTraceRocketMQMessage(){
        rocketMQTemplate.syncSend("traceTopic", "我是消息轨迹");
    }

}
