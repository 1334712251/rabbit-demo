package com.example.rabbit.six;

import com.example.rabbit.utils.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class ReceiveLogsDirect01 {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws Exception {

        Channel channel = RabbitMqUtils.getChannel();
        //声明一个交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        //声明一个队列
        String queueName = "disk";
        channel.queueDeclare(queueName, false, false, false, null);
        //队列与交换机绑定，routingKey为error
        channel.queueBind(queueName, EXCHANGE_NAME, "error");


        DeliverCallback deliverCallback = (consumerTag, delivery) -> {

            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            message = "接收绑定键:" + delivery.getEnvelope().getRoutingKey() + ",消息:" + message;
            System.out.println(message);
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}