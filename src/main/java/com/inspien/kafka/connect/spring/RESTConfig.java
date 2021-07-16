package com.inspien.kafka.connect.spring;

import java.util.HashMap;
import java.util.Map;

import com.inspien.kafka.connect.RESTContextManager;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

//TODO this class will be removed and only application and template will remain. Transfer all the instancing procedure into application.
/**
 * Config of REST API, generated using properties passed by {@link SpringAPIHandler}.
 */
@Configuration
//scan only spring api package to prevent multiple singleton instances exist
@ComponentScan(basePackages = {"com.inspien.kafka.connect.spring"})
public class RESTConfig {
    @Value("${synk.bootstrap-servers:localhost:9092}")
    public String bootstrapServers;

    @Value("${synk.name}")
    public String connectionId;

    @Value("${synk.suffix.request-topic:_requests}")
    public String requestSuffix;

    @Value("${synk.suffix.response-topic:_responses}")
    public String responseSuffix;
/*
    //we don't need producer for now
    @Value("${synk.suffix.producer:_source}")
    public String producerSuffix;
*/
    @Value("${synk.suffix.consumer:_sourcelistener}")
    public String consumerSuffix;

    @Value("${synk.suffix.consumergroup:_sourcelisteners}")
    public String consumerGroupSuffix;
    
    /**
     * Receive Config data from Registry.
     * @return {@link SourceConnectorConfig} of mother connector, stored in {@link RESTContextManager}
     */
    @Bean
    public AbstractConfig connectorConfig() {
        return RESTContextManager.getInstance().getConnectorConfig(connectionId);
    }
    /*
    //we do not need TransformChain now
    private TransformationChain<SourceRecord> transformationChain;
    @Bean
    public TransformationChain<SourceRecord> transformation(){
        //do not generate multiple time
        if (transformationChain == null){
            RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(
                    connectorConfig().errorRetryTimeout(),
                    connectorConfig().errorMaxDelayInMillis(),
                    connectorConfig().errorToleranceType(), Time.SYSTEM);
            transformationChain =  new TransformationChain<SourceRecord>(connectorConfig().transformations(),
                                                                        retryWithToleranceOperator);
        }
        return transformationChain;
    }
    */

    /*
    //We do not Producer now
    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> producerProps = new HashMap<>();

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        // These settings will execute infinite retries on retriable exceptions. They *may* be overridden via configs passed to the worker,
        // but this may compromise the delivery guarantees of Kafka Connect.
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, connectionId+producerSuffix);
        // Connector_specific overrides
        producerProps.putAll(connectorConfig().originalsWithPrefix("producer."));
        return producerProps;
    }

    @Bean
    public ProducerFactory<String, SourceRecord> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, SourceRecord> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


    */
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, connectionId+consumerGroupSuffix);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // Connector_specific overrides
        consumerProps.putAll(connectorConfig().originalsWithPrefix("consumer."));
        return consumerProps;
    }

    @Bean
    public ReplyingKafkaConnectTemplate replyKafkaTemplate(
            KafkaMessageListenerContainer<byte[], byte[]> container) {
        return new ReplyingKafkaConnectTemplate(container, connectionId);
    }

    @Bean
    public KafkaMessageListenerContainer<byte[], byte[]> replyContainer(ConsumerFactory<byte[], byte[]> cf) {
        ContainerProperties containerProperties = new ContainerProperties(responseSuffix);
        return new KafkaMessageListenerContainer<>(cf, containerProperties);
    }

    @Bean
    public ConsumerFactory<byte[], byte[]> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }
}