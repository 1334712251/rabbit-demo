package com.example.rabbit.four;

import com.example.rabbit.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;

import java.nio.charset.StandardCharsets;
import java.util.UUID;


/**
 * 发布确认模式
 * 1.单个确认
 * 2.批量确认
 * 3.异步批量确认
 */
public class ConfirmMessage {

    private static final int MESSAGE_COUNT = 1000;

    public static void main(String[] args) throws Exception {
//        ConfirmMessage.publishMessageIndividually();    //发布1000个单独确认消息,耗时31787ms
        ConfirmMessage.publishMessageBatch();    //发布1000个批量确认消息,耗时450ms
    }

    public static void publishMessageIndividually() throws Exception {

        Channel channel = RabbitMqUtils.getChannel();
        //UUID生成queue的名称
        String queueName = UUID.randomUUID().toString();
        /**
         * 生成一个队列
         * 1.队列名称
         * 2.队列里面的消息是否持久化，持久化是保存在磁盘中（默认消息存储在内存中），true保存在磁盘中，false不保存在磁盘中
         * 3.该队列是否只供一个消费者进行消费 是否进行共享, true 可以多个消费者消费，false不允许多个消费者消费，只允许一个
         * 4.队列是否自动删除 最后一个消费者端开连接以后 该队列是否自动删除 true 自动删除，false不自动删除
         * 5.其他参数
         */
        channel.queueDeclare(queueName, false, false, false, null);
        //开启发布确认
        channel.confirmSelect();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = i + "";
            channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
            //服务端返回 false 或超时时间内未返回，生产者可以消息重发
            boolean flag = channel.waitForConfirms();
            if (flag) {
                System.out.println("消息发送成功");
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("发布" + MESSAGE_COUNT + "个单独确认消息,耗时" + (end - begin) + "ms");
    }


    public static void publishMessageBatch() throws Exception {

        Channel channel = RabbitMqUtils.getChannel();
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName, false, false, false, null);
        //开启发布确认
        channel.confirmSelect();
        //批量确认消息大小
        int batchSize = 99;
        //未确认消息个数
        int outstandingMessageCount = 0;
        long begin = System.currentTimeMillis();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = i + "";
            channel.basicPublish("", queueName, null, message.getBytes());
            outstandingMessageCount++;
            if (outstandingMessageCount == batchSize) {
                channel.waitForConfirms();
                outstandingMessageCount = 0;
            }
        }
        //为了确保还有剩余没有确认消息 再次确认
        if (outstandingMessageCount > 0) {
            channel.waitForConfirms();
            System.out.println("再次确认");
        }
        long end = System.currentTimeMillis();
        System.out.println("发布" + MESSAGE_COUNT + "个批量确认消息,耗时" + (end - begin) + "ms");
    }
}
