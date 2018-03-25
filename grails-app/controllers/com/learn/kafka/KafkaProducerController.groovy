package com.learn.kafka

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

class KafkaProducerController {

    def index() {
        def topic = 'topic'
        def properties = [:]

        properties["bootstrap.servers"]='localhost:9092'
        properties["serializer.class"]='kafka.serializer.DefaultEncoder'
        properties["key.serializer"]='org.apache.kafka.common.serialization.StringSerializer'
        properties["value.serializer"]='org.apache.kafka.common.serialization.StringSerializer'

        String messageToSend="TTT"
        KafkaProducer kafkaProducer = new KafkaProducer(properties)
        ProducerRecord record = new ProducerRecord(topic, messageToSend)
        kafkaProducer.send(record);
    }
}
