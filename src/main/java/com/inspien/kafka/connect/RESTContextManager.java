package com.inspien.kafka.connect;

import java.util.HashMap;
import java.util.Map;

import com.inspien.kafka.connect.spring.RESTApplication;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.connect.data.Schema;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.util.SocketUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;



/**
 * Singleton manager class for all context info of REST APIs.
 * This include context registry, and manages the lifecycle of {@link TaskLoadBalancer}s and web application.
 * 
 */
@Slf4j
public class RESTContextManager {
//TODO For distributed work, RESTContextManager of each cluster node must be communicate with each other and synchronized.
//this could be achieved by Kafka Connect's way - using special topic for this cluster, consume the event in that topic.

    //region initialize
    public enum SchemaType{
        REQUEST, RESPONSE
    }
    private static RESTContextManager instance = new RESTContextManager();
    private Map<String,ConnectionContext> contexts;
    private ConfigurableApplicationContext webContext;

    class ConnectionContext{
        @Getter @Setter private Schema requestSchema;
        @Getter @Setter private Schema responseSchema;
        @Getter @Setter private AbstractConfig config;
        @Getter @Setter private TaskLoadBalancer taskLoadBalancer;
        @Getter @Setter private ReplyingKafkaConnectTemplate template;
    }

    private RESTContextManager(){
        contexts = new HashMap<>();
    }

    public ReplyingKafkaConnectTemplate getTemplate(String key){
        return this.getConnectorContext(key).getTemplate();
    }

    public static RESTContextManager getInstance(){
        return instance;
    }
    //endregion
    
    private synchronized ConnectionContext getConnectorContext(String key){
        if (!(this.contexts.keySet().contains(key))) {
            log.error("unregistered connector {} requests setting. null will be returned", key);
        }
        return this.contexts.get(key);
    }

    //region schema
    public void registerSchema(String key, Schema schema, SchemaType type){
        switch(type){
            case RESPONSE:
                this.registerResponseSchema(key, schema);
                break;
            case REQUEST:
                this.registerRequestSchema(key, schema);
                break;
            default:
                throw new SchemaException(String.format("schema type %s does not exist", type));
        }
    }

    public Schema getSchema(String key, SchemaType type){
        switch(type){
            case RESPONSE:
                return responseSchema(key);
            case REQUEST:
                return requestSchema(key);
            default:
                return null;
        }
    }
    public Schema requestSchema(String key){
        return this.getConnectorContext(key).getRequestSchema();
    }
    public Schema responseSchema(String key){
        return this.getConnectorContext(key).getResponseSchema();
    }

    public void registerRequestSchema(String key, Schema schema){
        this.getConnectorContext(key).setRequestSchema(schema);
    }
    public void registerResponseSchema(String key, Schema schema){
        this.getConnectorContext(key).setResponseSchema(schema);
    }
    //endregion
    
    /**
     * register connector to the registry. 
     * @param key registry key, usually connector's {@code ConnectionId}.
     * @param config config of the connector. this will be accessed by other objects for each request.
     */
    public void registerConnector(String key, AbstractConfig config){
        if (!(this.contexts.keySet().contains(key))){
            log.trace("connector {} is registered with following settings: {}",key,config.toString());
            this.contexts.put(key, new ConnectionContext());
        } 
        this.getConnectorContext(key).setConfig(config);
        launchWebAppIfNotLaunched();
        generateTemplateIfNotExist(key);
    }
    
    public void deregisterConnector(String key){
        this.contexts.remove(key);
        this.taskLoadBalancer(key).close();
        this.removeTemplate(key);
        //if there are no connector, close webserver.
        if (this.contexts.size() <= 0){
            this.webContext.close();
        }
    }

    public boolean isRegistered(String key){
        return this.contexts.keySet().contains(key);
    }

    /**
     * Generate webapp using {@link SpringApplicationBuilder} if no {@link ConfigurableApplicationContext} is assigned for this server.
     * By default, This will use environmental variable "{@code SYNK_PORT}" or 8041 if no settings, But other port will be assigned if that port is in use.
     * That auto-aligned port will probably blocked by firewall, so must be checked.
     */
    private void launchWebAppIfNotLaunched(){
        if (this.webContext == null){
            Integer port = null;
            try{
                port = Integer.parseInt(System.getenv("SYNK_PORT"));
            }
            catch(Exception e){
                log.error("Cannot get environment variable SYNK_PORT, by following exception : {}",e.toString());
            }
            if (port==null){
                log.warn("Failed finding System environment variable. Default port 8041 will be used for the web server");
                port = 8041;
            }
            if(!Utils.validatePortNumber(port)){
                log.warn("port {} is already in use.",port);
                port = SocketUtils.findAvailableTcpPort(port);
                log.warn("Nearleast port {} is selected for the connection. This port may be blocked by firewall.");
            }
            this.webContext = new SpringApplicationBuilder(RESTApplication.class).properties(String.format("server.port=%d",port))
                    .run();
            log.info("Web server launched at port {}",port);
            return;
        }
        if (!this.webContext.isActive()){
            log.warn("suspicious state of webcontext is detected (is not active) so web application will be rebooted");
            SpringApplication.exit(this.webContext, () -> 0 );
            this.webContext = null;
            this.launchWebAppIfNotLaunched();
        }
        if (!this.webContext.isRunning()){
            log.warn("Web server is closed so relaunch is tried. Previous context will be removed and will be relaunched");
            this.webContext.close();
            this.webContext = null;
            this.launchWebAppIfNotLaunched();
        }
    }

    /**
     * Generate {@link REplyingKafkaConnectTemplate} if not exist.
     * @param key registry key
     * @return generated/existing template
     */
    private ReplyingKafkaConnectTemplate generateTemplateIfNotExist(String key){
        //if exist, return exist refernece.
        if (this.getTemplate(key) != null) return this.getTemplate(key);

        //Set Consumer Configs
        String connectionId =  this.getConfigFieldString(key, RESTSyncConnector.CONNECTION_ID);
        String topic = connectionId + this.getConfigFieldString(key, RESTSyncConnector.RESPONSE_TOPIC_SUFFIX);
        String consumerGroup = connectionId + this.getConfigFieldString(key, RESTSyncConnector.CONSUMER_GROUP_SUFFIX) +
                                Utils.getLocalIp();
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getConfigFieldString(key, RESTSyncConnector.BOOTSTRAP_SERVER));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // Connector_specific overrides
        consumerProps.putAll(this.getConnectorConfig(key).originalsWithPrefix("consumer."));

        //generate containerproperties using topic
        ContainerProperties containerProperties = new ContainerProperties(topic);
        //generate container
        KafkaMessageListenerContainer<byte[],byte[]> container =
            new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(consumerProps), containerProperties);
        //generate template and register it
        ReplyingKafkaConnectTemplate template =  new ReplyingKafkaConnectTemplate(container, connectionId);
        this.getConnectorContext(key).setTemplate(template);
        return template;
    }

    /**
     * Remove template. template will be stopped to unload all thread, and then its reference will be deleted.
     * @param key registry id
     */
    private void removeTemplate(String key){
        ReplyingKafkaConnectTemplate template = this.getTemplate(key);
        //nothing have to do if template already stopped
        if (template == null) return;
        template.stop();
        //remove template reference - GC will take it if no reference left
        this.getConnectorContext(key).setTemplate(null);
    }

    public void registerLB(String key, TaskLoadBalancer lb){
        this.getConnectorContext(key).setTaskLoadBalancer(lb);
    }

    public boolean isLBLaunched(String key){
        return this.taskLoadBalancer(key) != null;
    }

    public AbstractConfig getConnectorConfig(String key){
        return this.getConnectorContext(key).getConfig();
    }

    public TaskLoadBalancer taskLoadBalancer(String key){
        return this.getConnectorContext(key).getTaskLoadBalancer();
    }

    public boolean isWebServerLaunched(){
        return this.webContext != null;
    }


    public String getConfigFieldString(String key, String field){
        return this.getConnectorConfig(key).getString(field);
    }
    
}
