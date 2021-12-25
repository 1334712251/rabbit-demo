package com.example.rabbit.one;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Producer {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        //创建一个连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("120.26.84.226");
        factory.setUsername("guest");
        factory.setPassword("guest");

        //创建连接
        Connection connection = factory.newConnection();
        //创建通道
        Channel channel = connection.createChannel();

        /**
         * 生成一个队列
         * 1.队列名称
         * 2.队列里面的消息是否持久化，持久化是保存在磁盘中（默认消息存储在内存中），true保存在磁盘中，false不保存在磁盘中
         * 3.该队列是否只供一个消费者进行消费 是否进行共享, true 可以多个消费者消费，false不允许多个消费者消费，只允许一个
         * 4.是否自动删除 最后一个消费者端开连接以后 该队列是否自动删除 true 自动删除，false不自动删除
         * 5.其他参数
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "hello world";


        /**
         * 发送消息
         * 1.发送到那个交换机
         * 2.路由的 key 值是哪个，本次是队列名称
         * 3.其他的参数信息
         * 4.发送消息的消息体
         */
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());

    }
}
