package com.learn.kafka

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;

class KafkaConsumerController {

    def index() {

        Properties props = new Properties();
        props.put("zookeeper.connect", 'localhost:2181');
        props.put("group.id", 'topic');
        props.put("zookeeper.session.timeout.ms", "1000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props))

        def consumerMap = consumer.createMessageStreams(['topic':1])
        def streams = consumerMap.get('topic')

        streams.each{m_stream ->
            ConsumerIterator iter = m_stream.iterator()
            while(iter.hasNext()){
                def message = new String(iter.next().message())
                println message
            }
        }
    }
}
