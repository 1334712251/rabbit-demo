package com.example.rabbit.four;

import com.example.rabbit.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;

import java.nio.charset.StandardCharsets;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;


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
//        ConfirmMessage.publishMessageBatch();    //发布1000个批量确认消息,耗时450ms
        ConfirmMessage.publishMessageAsync();    //发布1000个异步确认消息,耗时72ms
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


    public static void publishMessageAsync() throws Exception {


        Channel channel = RabbitMqUtils.getChannel();
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName, false, false, false, null);
        //开启发布确认
        channel.confirmSelect();
        /**
         * 线程安全有序的一个哈希表，适用于高并发的情况
         * 1.轻松的将序号与消息进行关联
         * 2.轻松批量删除条目 只要给到序列号
         * 3.支持并发访问
         */
        ConcurrentSkipListMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();
        /**
         * 确认收到消息的一个回调
         *
         * 1.sequenceNumber:消息的序列号
         * 2.multiple:是否为批量确认
         *
         * 1.消息序列号
         * 2.true 可以确认小于等于当前序列号的消息
         * false 确认当前序列号消息
         */

        AtomicInteger flag = new AtomicInteger();
        ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
            System.out.println("标记为" + sequenceNumber + "是否批量" + multiple);
//            System.out.println(flag.getAndIncrement());
//            String message = outstandingConfirms.get(sequenceNumber);
//            System.out.println("发布的消息" + message + "已被确认，序列号为" + sequenceNumber);
            //删除已确认的信息，剩下的就是未确认的信息
            //multiple:是否为批量确认
            if (multiple) {
//                ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
                //拿到的消息是被确认的消息
                //headMap方法通过sequenceNumber标记找到相关联的aLong序号，然后一起批量clear掉
                ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber);
                NavigableSet<Long> longs = confirmed.keySet();
                for (Long aLong : longs) {
                    String s = confirmed.get(aLong);
//                    System.out.println("确认的消息" + s + "序号为" + aLong);
//                    System.out.println("标记为" + sequenceNumber + "序号为" + aLong);
                }
                //消除被批量确认的消息
                confirmed.clear();
            } else {
                //不是批量确认消息,只清除当前序列号的消息
                outstandingConfirms.remove(sequenceNumber);
            }
        };

        /**
         * 1.sequenceNumber:消息的序列号
         * 2.multiple:是否为批量确认
         */
        ConfirmCallback nackCallback = (sequenceNumber, multiple) -> {
            String message = outstandingConfirms.get(sequenceNumber);
            System.out.println("发布的消息" + message + "未被确认，序列号为" + sequenceNumber);
        };
        /**
         * 添加一个异步确认的监听器
         * 1.确认收到消息的回调
         * 2.未收到消息的回调
         */
//        channel.addConfirmListener(ackCallback, null);
        channel.addConfirmListener(ackCallback, nackCallback);
        long begin = System.currentTimeMillis();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = "消息" + i;
            /**
             * channel.getNextPublishSeqNo()获取下一个消息的序列号
             * 通过序列号与消息体进行一个关联
             * 全部都是未确认的消息体
             */
            outstandingConfirms.put(channel.getNextPublishSeqNo(), message);
            channel.basicPublish("", queueName, null, message.getBytes());
//            System.out.println("序列号为" + channel.getNextPublishSeqNo());
//            System.out.println("序列号----为" + channel.getNextPublishSeqNo());
        }
        long end = System.currentTimeMillis();
        System.out.println("发布" + MESSAGE_COUNT + "个异步确认消息,耗时" + (end - begin) + "ms");
    }
}
