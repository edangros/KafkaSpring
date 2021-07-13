package com.inspien.kafka.connect;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.connect.data.Schema;
import org.springframework.context.ConfigurableApplicationContext;

import lombok.Getter;
import lombok.Setter;



/**
 * registry for all context info of REST APIs.
 * Contexts including Connector Settings, applied Tasks, Web Application Context are stored here.
 * 
 */

public class RESTContextRegistry {

    public enum SchemaType{
        WEB_REQUEST, KAFKA_REQUEST, KAFKA_RESPONSE
    }
    private static RESTContextRegistry instance = new RESTContextRegistry();
    private Map<String,RESTContext> contexts;

    class RESTContext{
        @Getter @Setter private Schema webRequestSchema;
        @Getter @Setter private Schema kafkaRequestSchema;
        @Getter @Setter private Schema kafkaResponseSchema;
        @Getter @Setter private AbstractConfig config;
        @Getter @Setter private ConfigurableApplicationContext webContext;
        @Getter @Setter private TaskLoadBalancer taskLoadBalancer;
    }

    private RESTContextRegistry(){
        contexts = new HashMap<>();
    }

    public static RESTContextRegistry getInstance(){
        return instance;
    }

    private RESTContext getConnectorSettings(String key){
        if (!(this.contexts.keySet().contains(key))) this.contexts.put(key, new RESTContext());
        return this.contexts.get(key);
    }
    public Schema webRequestSchema(String key){
        return this.getConnectorSettings(key).getWebRequestSchema();
    }
    public Schema kafkaRequestSchema(String key){
        return this.getConnectorSettings(key).getKafkaRequestSchema();
    }
    public Schema kafkaResponseSchema(String key){
        return this.getConnectorSettings(key).getKafkaResponseSchema();
    }

    public void registerWebRequestSchema(String key, Schema schema){
        this.getConnectorSettings(key).setWebRequestSchema(schema);
    }
    public void registerKafkaRequestSchema(String key, Schema schema){
        this.getConnectorSettings(key).setKafkaRequestSchema(schema);
    }
    public void registerKafkaResponseSchema(String key, Schema schema){
        this.getConnectorSettings(key).setKafkaResponseSchema(schema);
    }

    public Schema getSchema(String key, SchemaType type){
        switch(type){
            case KAFKA_REQUEST:
                return kafkaRequestSchema(key);
            case KAFKA_RESPONSE:
                return kafkaResponseSchema(key);
            case WEB_REQUEST:
                return webRequestSchema(key);
            default:
                return null;
        }
    }
    
    public void registerSchema(String key, Schema schema, SchemaType type){
        switch(type){
            case KAFKA_REQUEST:
                this.registerKafkaRequestSchema(key, schema);
                break;
            case KAFKA_RESPONSE:
                this.registerKafkaResponseSchema(key, schema);
                break;
            case WEB_REQUEST:
                this.registerWebRequestSchema(key, schema);
                break;
            default:
                throw new SchemaException(String.format("schema type %s does not exist", type));
        }
    }

    public void registerConnector(String key, AbstractConfig config){
        this.getConnectorSettings(key).setConfig(config);
    }
    
    public void registerApplication(String key, ConfigurableApplicationContext context){
        this.getConnectorSettings(key).setWebContext(context);
    }

    public void registerLB(String key, TaskLoadBalancer lb){
        this.getConnectorSettings(key).setTaskLoadBalancer(lb);
    }

    public ConfigurableApplicationContext webApplication(String key){
        return this.getConnectorSettings(key).getWebContext();
    }

    public AbstractConfig getConnectorConfig(String key){
        return this.getConnectorSettings(key).getConfig();
    }

    public TaskLoadBalancer taskLoadBalancer(String key){
        return this.getConnectorSettings(key).getTaskLoadBalancer();
    }

    public boolean isWebServerLaunched(String key){
        return this.webApplication(key) != null;
    }

    public boolean isLBLaunched(String key){
        return this.taskLoadBalancer(key) != null;
    }

    public void deregisterConnector(String key){
        this.contexts.remove(key);
    }

    public boolean isRegistered(String key){
        return this.contexts.keySet().contains(key);
    }
}
