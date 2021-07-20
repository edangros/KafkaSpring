package com.inspien.kafka.connect;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.transforms.Transformation;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Connector to connect Rest endpoint to kafka, with synchronized communication.
 * Could be parallelized as much as system allows, using simple loadbalancer which controlles the load of each tasks to process message.
 * This parallelization allows complex transform performed via {@link Transformation} API, but heavy transformation and high {@code tasks.max} is not recommended.
 */
@Slf4j
public class MirrorSyncSinkConnector extends SinkConnector {
    public static final Map<String,KafkaProducer<byte[],byte[]>> producerRegistry = new HashMap<>();
    public static final String CONNECTION_ID = "synk.name";
    public static final String CONNECTION_ID_DOC = "Connection ID for this connection. must be unique for all connection.\n"+
                                                    "this used to generate Topic, consumer, and consumer group so must not have any character's kafka support.";
    public static final String BOOTSTRAP_SERVER = "synk.bootstrap-servers";
    public static final String RESPONSE_TOPIC = "mirror.response-topic";
    public static final String PRODUCER_SUFFIX = "synk.suffix.producer";


    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CONNECTION_ID, Type.STRING, null, Importance.HIGH, CONNECTION_ID_DOC)
        .define(BOOTSTRAP_SERVER, Type.STRING, "localhost:9092", Importance.HIGH, "Bootstrap server of response topic. COULD BE DIFFERENT FROM KAFKA CONNECT's BOOTSTRAPs.")
        .define(PRODUCER_SUFFIX, Type.STRING, "_mirror", Importance.MEDIUM, "The suffix of producer id")
        .define(RESPONSE_TOPIC, Type.STRING, null, Importance.MEDIUM, "The name of response topic");

    private String connectionId;
    private String responseTopic;
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        this.connectionId = parsedConfig.getString(ConnectorConfig.NAME_CONFIG);
        this.responseTopic = parsedConfig.getString(RESPONSE_TOPIC);
        Map<String,Object> producerProps = new HashMap<>();
        
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.join(parsedConfig.getList(BOOTSTRAP_SERVER), ","));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        // These settings will execute infinite retries on retriable exceptions. They *may* be overridden via configs passed to the worker,
        // but this may compromise the delivery guarantees of Kafka Connect.
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, this.connectionId+parsedConfig.getString(PRODUCER_SUFFIX));
        // User-specified overrides
        producerProps.putAll(parsedConfig.originalsWithPrefix("mirror.producer."));
        // register producer
        producerRegistry.put(this.connectionId, new KafkaProducer<>(producerProps));
        log.info("producer for mirror {} is created",this.connectionId);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RESTInputSourceTask.class;
    }
    
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        //NOTE the number of tasks is not determined by maxTasks value in config, but the size of the list we return in this function.
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        //we'll generate tasks by number of maxTasks.
        for(int i=0; i<maxTasks; i++){
            Map<String,String> config = new HashMap<>();
            config.put(ConnectorConfig.NAME_CONFIG, connectionId); //They have to access the producer
            config.put(RESPONSE_TOPIC, this.responseTopic);
            configs.add(config);
        }
        return configs;
    }
    
    @Override
    public void stop() {
        //deregister producer
        producerRegistry.remove(this.connectionId);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
