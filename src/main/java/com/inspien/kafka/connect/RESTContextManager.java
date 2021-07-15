package com.inspien.kafka.connect;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

import com.inspien.kafka.connect.spring.RESTApplication;
import com.inspien.kafka.connect.spring.ReplyingKafkaConnectTemplate;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.connect.data.Schema;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.SocketUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;



/**
 * registry for all context info of REST APIs.
 * Contexts including Connector Settings, applied Tasks, Web Application Context are stored here.
 * 
 */
@Slf4j
public class RESTContextManager {

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
        @Getter @Setter private ConfigurableApplicationContext webContext;
        @Getter @Setter private TaskLoadBalancer taskLoadBalancer;
        @Getter @Setter private ReplyingKafkaConnectTemplate template;
    }

    private RESTContextManager(){
        contexts = new HashMap<>();
    }

    public ReplyingKafkaConnectTemplate getTemplate(String key){
        return this.getConnectionContext(key).getTemplate();
    }

    public static RESTContextManager getInstance(){
        return instance;
    }

    private synchronized ConnectionContext getConnectionContext(String key){
        if (!(this.contexts.keySet().contains(key))) {
            log.error("unregistered connector {} requests setting. null will be returned", key);
        }
        return this.contexts.get(key);
    }

    public Schema requestSchema(String key){
        return this.getConnectionContext(key).getRequestSchema();
    }
    public Schema responseSchema(String key){
        return this.getConnectionContext(key).getResponseSchema();
    }

    public void registerRequestSchema(String key, Schema schema){
        this.getConnectionContext(key).setRequestSchema(schema);
    }
    public void registerResponseSchema(String key, Schema schema){
        this.getConnectionContext(key).setResponseSchema(schema);
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

    public void registerConnector(String key, AbstractConfig config){
        if (!(this.contexts.keySet().contains(key))){
            log.trace("connector {} is registered with following settings: {}",key,config.toString());
            this.contexts.put(key, new ConnectionContext());
        } 
        this.getConnectionContext(key).setConfig(config);
        launchWebAppIfNotLaunched();
        if(!this.webContext.isRunning()){
            this.webContext.start();
        }
    }
    
    /**
     * Generate webapp using {@link SpringApplicationBuilder} if no {@link ConfigurableApplicationContext} is assigned for this server.
     * By default, This will use environmental variable "{@code SYNK_PORT}" or 8041 if no settings, But other port will be assigned if that port is in use.
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
            if(!validatePortNumber(port)){
                log.warn("port {} is already in use.",port);
                port = SocketUtils.findAvailableTcpPort(port);
                log.warn("Nearleast port {} is selected for the connection. This port may be blocked by firewall.");
            }
            this.webContext = new SpringApplicationBuilder(RESTApplication.class).properties(String.format("server.port=%d",port))
                    .run();
        }
        
    }

    /**
     * Check if the port is in use
     * @param 
     * @return
     */
    private boolean validatePortNumber(int port){
        try (ServerSocket serverSocket = new ServerSocket()) {
            // setReuseAddress(false) is required only on OSX, 
            // otherwise the code will not work correctly on that platform          
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
    
    public void registerApplication(String key, ConfigurableApplicationContext context){
        this.getConnectionContext(key).setWebContext(context);
    }

    public void registerLB(String key, TaskLoadBalancer lb){
        this.getConnectionContext(key).setTaskLoadBalancer(lb);
    }

    public ConfigurableApplicationContext webApplication(String key){
        return this.getConnectionContext(key).getWebContext();
    }

    public AbstractConfig getConnectorConfig(String key){
        return this.getConnectionContext(key).getConfig();
    }

    public TaskLoadBalancer taskLoadBalancer(String key){
        return this.getConnectionContext(key).getTaskLoadBalancer();
    }

    public boolean isWebServerLaunched(String key){
        return this.webApplication(key) != null;
    }

    public boolean isLBLaunched(String key){
        return this.taskLoadBalancer(key) != null;
    }

    public void deregisterConnector(String key){
        this.contexts.remove(key);
        //if there are no connector, webserver must be closed
        if (this.contexts.size() <= 0){
            this.webContext.close();
        }
    }

    public String getConfigFieldString(String key, String field){
        return this.getConnectorConfig(key).getString(field);
    }
    
    public boolean isRegistered(String key){
        return this.contexts.keySet().contains(key);
    }
}
