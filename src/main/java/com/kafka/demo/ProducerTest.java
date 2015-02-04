package com.kafka.demo;

import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerTest extends Thread {
    private final Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();
    List<String> json;
    int idx = 0;

    public ProducerTest(String topic, List<String> json) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("request.required.acks", "1");
//		props.put("broker.list", "0:127.0.0.1:9092");
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
        this.json = json;
    }

    public void run() {
        while (true) {
            String messageStr = new String(json.get(idx++ % json.size()));
//            System.out.println(topic+"   __________    "+messageStr);
            KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic,messageStr);
            producer.send(data);
//            producer.send(new ProducerData<Integer, String>(topic, messageStr));
        }
    }

}
