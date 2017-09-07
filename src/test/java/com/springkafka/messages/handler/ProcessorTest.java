package com.springkafka.messages.handler;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class ProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorTest.class);

    private static String RECEIVER_TOPIC = "receiver.t";
    private static String SENDER_TOPIC = "sender.t";

    private static long NUMBER_TEST1_1 = 23456;
    private static long NUMBER_TEST1_2 = 675475;
    private static long NUMBER_TEST1_3 = 3425345;

    private static long NUMBER_TEST2_1 = 3453245;
    private static long NUMBER_TEST2_2 = 123;

    private static long NUMBER_TEST3_1 = 33453;

    private static long NUMBER_TEST4_1 = 7645;
    private static long NUMBER_TEST4_2 = 3434;
    private static long NUMBER_TEST4_3 = 234234234;
    private static long NUMBER_TEST4_4 = 3545;
    private static long NUMBER_TEST4_5 = 234234;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, SENDER_TOPIC, RECEIVER_TOPIC);

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private KafkaTemplate<String, String> template;

    private BlockingQueue<ConsumerRecord<String, String>> records;

    private KafkaMessageListenerContainer<String, String> container;

    @Before
    public void setUp() throws Exception {
        setUpProducer();
        setUpConsumer();
    }

    @After
    public void tearDown() {
        // stop the container
        container.stop();
    }


    private void setUpConsumer() throws Exception {
        // set up the Kafka consumer properties
        Map<String, Object> consumerProperties =
        KafkaTestUtils.consumerProps("sender", "false", embeddedKafka);

        // create a Kafka consumer factory
        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProperties);

        // set the topic that needs to be consumed
        ContainerProperties containerProperties = new ContainerProperties(SENDER_TOPIC);

        // create a Kafka MessageListenerContainer
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        // create a thread safe queue to store the received message
        records = new LinkedBlockingQueue<>();

        // setup a Kafka message listener
        container.setupMessageListener((MessageListener<String, String>) record -> {
            LOGGER.debug("test-listener received message='{}'", record.toString());
            records.add(record);
        });

        // start the container and underlying message listener
        container.start();

        // wait until the container has the required number of assigned partitions
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }

    private void setUpProducer() throws Exception {
        // set up the Kafka producer properties
        Map<String, Object> senderProperties =
        KafkaTestUtils.senderProps(embeddedKafka.getBrokersAsString());

        // create a Kafka producer factory
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(senderProperties);

        // create a Kafka template
        template = new KafkaTemplate<>(producerFactory);
        // set the default topic to send to
        template.setDefaultTopic(RECEIVER_TOPIC);

        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                                                                 .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer,
                                                 embeddedKafka.getPartitionsPerTopic());
        }
    }

    @Test
    public void given_messageFromQueueWithCommaSeparatedIntegers_when_process_should_returnJsonObject_userCase1() throws Exception {
        // send the message
        final String message = Stream.of(NUMBER_TEST1_1, NUMBER_TEST1_2, NUMBER_TEST1_3)
                               .map(Object::toString)
                               .collect(Collectors.joining(","));
        template.sendDefault(message);
        LOGGER.debug("test-sender sent message='{}'", message);

        final ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasValue("{\"numberOfValues\":" + 3 + ",\"sumOfValues\":" + (NUMBER_TEST1_3 + NUMBER_TEST1_2 + NUMBER_TEST1_1) + "}"));
    }

    @Test
    public void given_messageFromQueueWithCommaSeparatedIntegers_when_process_should_returnJsonObject_userCase2() throws Exception {
        // send the message
        final String message = Stream.of(NUMBER_TEST2_1, NUMBER_TEST2_2)
                               .map(Object::toString)
                               .collect(Collectors.joining(","));
        template.sendDefault(message);
        LOGGER.debug("test-sender sent message='{}'", message);

        final ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasValue("{\"numberOfValues\":" + 2 + ",\"sumOfValues\":" + (NUMBER_TEST2_1 + NUMBER_TEST2_2) + "}"));
    }
    @Test
    public void given_messageFromQueueWithCommaSeparatedIntegers_when_process_should_returnJsonObject_userCase3() throws Exception {
        // send the message
        final String message = Stream.of(NUMBER_TEST3_1)
                                     .map(Object::toString)
                                     .collect(Collectors.joining(","));
        template.sendDefault(message);
        LOGGER.debug("test-sender sent message='{}'", message);

        final ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasValue("{\"numberOfValues\":" + 1 + ",\"sumOfValues\":" + (NUMBER_TEST3_1) + "}"));
    }

    @Test
    public void given_messageFromQueueWithCommaSeparatedIntegers_when_process_should_returnJsonObject_userCase4() throws Exception {
        // send the message
        final String message = Stream.of(NUMBER_TEST4_1, NUMBER_TEST4_2, NUMBER_TEST4_3, NUMBER_TEST4_4, NUMBER_TEST4_5)
                                     .map(Object::toString)
                                     .collect(Collectors.joining(","));
        template.sendDefault(message);
        LOGGER.debug("test-sender sent message='{}'", message);

        final ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received, hasValue("{\"numberOfValues\":" + 5 + ",\"sumOfValues\":" + (NUMBER_TEST4_1 + NUMBER_TEST4_2 + NUMBER_TEST4_3 + NUMBER_TEST4_4 + NUMBER_TEST4_5) + "}"));
    }
}
