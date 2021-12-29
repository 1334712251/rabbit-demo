package com.example.rabbit.two;

import com.example.rabbit.utils.RabbitMqUtils;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class Worker01 {
    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String receivedMessage = new String(delivery.getBody());
            System.out.println("接收到消息:" + receivedMessage);
            /**
             * 1.消息标记 tag
             * 2.是否批量应答未应答消息
             */
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);
            System.out.println("应答消息:" + receivedMessage);
        };

        CancelCallback cancelCallback = (consumerTag) -> {
            System.out.println(consumerTag + "消费者取消消费接口回调逻辑");
        };

        System.out.println("C2 消费者启动等待消费.................. ");
        //设置不公平分发
//        channel.basicQos(1);
        //预期值是2，2以后都是预期值
        channel.basicQos(2);
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, cancelCallback);
    }
}