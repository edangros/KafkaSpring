/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inspien.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.inspien.kafka.connect.spring.RESTApplication;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class RESTSyncConnector extends SourceConnector {

    public static final String CONNECTION_ID = "synk.name";
    public static final String BOOTSTRAP_SERVER = "synk.bootstrap-servers";
    public static final String REQUEST_TOPIC_SUFFIX = "synk.suffix.request-topic";
    public static final String RESPONSE_TOPIC_SUFFIX = "synk.suffix.response-topic";
    public static final String PRODUCER_SUFFIX = "synk.suffix.producer";
    public static final String CONSUMER_SUFFIX = "synk.suffix.consumer";
    public static final String CONSUMER_GROUP_SUFFIX = "synk.suffix.consumergroup";
    public static final String SCHEMAPOLICY_WEBREQUEST = "synk.schema.web-request";
    public static final String SCHEMAPOLICY_KAFKAREQUEST = "synk.schema.kafka-request";
    public static final String SCHEMAPOLICY_KAFKARESPONSE = "synk.schema.kafka-response";
    public static final String LOADBALANCER_SCORING = "synk.loadbalancer.scoring";
    public static final String WEBSERVER_PORT = "synk.api.port";
    public static final String LOADBALANCER_SCORING_DOC = "How to loadbalancer operate. \n"+
            "If set to 'BY_CNT', lb will work based on the remaining messages for each task.\n"+
            "If set to 'BY_SIZE' lb will work based on the total size of remaing messages for each task."+
            "BY_CNT is recommanded if transformation load is not heavy, while BY_SIZE is recommanded if there are many field-to-field transformations.\n"+
            "default is 'BY_CNT'.";


    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CONNECTION_ID, Type.STRING, null, Importance.HIGH, "Connection ID for this connection. must be unique for all connection.")
        .define(BOOTSTRAP_SERVER, Type.STRING, "localhost:9092", Importance.HIGH, "Bootstrap server of response topic. COULD BE DIFFERENT FROM CONNECT's BOOTSTRAPs.")
        .define(REQUEST_TOPIC_SUFFIX, Type.STRING, null, Importance.MEDIUM, "The suffix of request topic")
        .define(RESPONSE_TOPIC_SUFFIX, Type.STRING, null, Importance.MEDIUM, "The suffix of response topic")
        .define(CONSUMER_SUFFIX, Type.STRING, null, Importance.MEDIUM, "The suffix of consumer id")
        .define(CONSUMER_GROUP_SUFFIX, Type.STRING, null, Importance.MEDIUM, "The suffix of consumer group")
        .define(WEBSERVER_PORT,Type.INT,80,Importance.MEDIUM,"The port of web endpoint, defaults 80.")
        .define(SCHEMAPOLICY_WEBREQUEST, Type.STRING, "ONCE", Importance.LOW, "Schema Policy of web request. Default is ONCE.")
        .define(SCHEMAPOLICY_KAFKAREQUEST, Type.STRING, "ONCE", Importance.LOW, "Schema Policy of kafka request. Default is ONCE.")
        .define(SCHEMAPOLICY_KAFKARESPONSE, Type.STRING, "ONCE", Importance.LOW, "Schema Policy of kafka response. Default is ONCE.")
        .define(LOADBALANCER_SCORING, Type.STRING, null, Importance.LOW, LOADBALANCER_SCORING_DOC);

    private String connectionId;
    private String requestTopic;
    private String responseTopic;
    private TaskLoadBalancer loadBalancer;
    private String lbScoringMethod;

    private ConfigurableApplicationContext webContext;
    private String webRequestSchemaPolicy;
    private String kafkaRequestSchemaPolicy;
    private String kafkaResponseSchemaPolicy;
    private String requestTopicSuffix;
    private String responseTopicSuffix;
    private String consumerSuffix;
    private String consumerGroupSuffix;
    private int webPort;
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        this.connectionId = parsedConfig.getString(CONNECTION_ID);
        this.requestTopicSuffix = parsedConfig.getString(REQUEST_TOPIC_SUFFIX);
        this.responseTopicSuffix = parsedConfig.getString(RESPONSE_TOPIC_SUFFIX);
        this.consumerSuffix = parsedConfig.getString(CONSUMER_SUFFIX);
        this.consumerGroupSuffix = parsedConfig.getString(CONSUMER_GROUP_SUFFIX);
        this.lbScoringMethod = parsedConfig.getString(LOADBALANCER_SCORING);
        this.webRequestSchemaPolicy = parsedConfig.getString(SCHEMAPOLICY_WEBREQUEST);
        this.kafkaRequestSchemaPolicy = parsedConfig.getString(SCHEMAPOLICY_KAFKAREQUEST);
        this.kafkaResponseSchemaPolicy = parsedConfig.getString(SCHEMAPOLICY_KAFKARESPONSE);
        this.webPort = parsedConfig.getInt(WEBSERVER_PORT);
        this.requestTopic = connectionId+requestTopicSuffix;
        this.responseTopic = connectionId+responseTopicSuffix;
        
        //validate settings
        final String[] schema_policies = {"ONCE","PRESET","IGNORE"};
        if()
        if(RESTContextRegistry.getInstance().isRegistered(this.connectionId))
            throw new ConfigException(String.format("ConnectionId must be Unique : connection %s is already launched.",
                                                            this.connectionId));
        try{
            //launch webapp
            webContext = runAPI();
            //register webapp
            RESTContextRegistry.getInstance().registerApplication(this.connectionId, webContext);
            //generate LB
            loadBalancer = new TaskLoadBalancer(parsedConfig);
            RESTContextRegistry.getInstance().registerLB(this.connectionId, loadBalancer);
        }
        catch(Exception e){
            //remove contexts generated by this
            RESTContextRegistry.getInstance().deregisterConnector(this.connectionId);
            if (webContext != null) webContext.close();
            throw new ConfigException(String.format("Connection %s failed due to error",this.connectionId)+e.getMessage());
        }
        
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RESTInputSourceTask.class;
    }


    public static final String TASK_INDEX = "synk.task.id";
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for(int i=0; i<maxTasks; i++){
            Map<String,String> config = new HashMap<>();
            //send task id for log
            config.put(TASK_INDEX, String.valueOf(i));
            config.put(CONNECTION_ID, connectionId);
            config.put(LOADBALANCER_SCORING, lbScoringMethod);
            configs.add(config);
        }
        return configs;
    }
    

    public Map<String, Object> webConfigs() {
        Map<String, Object> config = new HashMap<>();
        config.put(REQUEST_TOPIC_SUFFIX, requestTopic);
        return config;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSourceConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    
    /**
     * Run Spring API Server and return context
     */
    public ConfigurableApplicationContext runAPI() {
        ConfigurableApplicationContext context;
        //create new instance and run it
        context = new SpringApplicationBuilder(RESTApplication.class).properties(this.webConfigs())
                .properties("server.servelet.context-path",this.connectionId)
                .properties("server.servelet.context-path",this.connectionId)
                .run();
        RESTContextRegistry.getInstance().registerApplication(this.connectionId, context);
        return context;
    }

    /**
     * Stop API.
     */
    public void stopAPI() {
        if (RESTContextRegistry.getInstance().isWebServerLaunched(this.connectionId))
            RESTContextRegistry.getInstance().webApplication(this.connectionId).close();
        RESTContextRegistry.getInstance().deregisterConnector(this.connectionId);
    }

}
