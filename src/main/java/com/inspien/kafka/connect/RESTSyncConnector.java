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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.transforms.Transformation;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Connector to connect Rest endpoint to kafka, with synchronized communication.
 * Could be parallelized as much as system allows, using simple loadbalancer which controlles the load of each tasks to process message.
 * This parallelization allows complex transform performed via {@link Transformation} API, but heavy transformation and high {@code tasks.max} is not recommended.
 */
@Slf4j
public class RESTSyncConnector extends SourceConnector {

    public static final String CONNECTION_ID = "synk.name";
    public static final String CONNECTION_ID_DOC = "Connection ID for this connection. must be unique for all connection.\n"+
                                                    "this used to generate Topic, consumer, and consumer group so must not have any character's kafka support.";
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
    public static final String LOADBALANCER_SCORING_DOC = "How to loadbalancer operate. \n"+
            "If set to 'BY_CNT', lb will work based on the remaining messages for each task.\n"+
            "If set to 'BY_SIZE' lb will work based on the total size of remaing messages for each task."+
            "BY_CNT is recommanded if transformation load is not heavy, while BY_SIZE is recommanded if there are many field-to-field transformations.\n"+
            "default is 'BY_CNT'.";
    public static final String SCHEMAPOLICY_DOC = "You can provide json schema in this field, or follow settings:\n"+
                                                "- ONCE : When receive first message, guess schema from that message. Once the schema is set, messages with different schema will generate error. \n"+
                                                "- IGNORE : Never check schema and generate schema every time based on the received message.\n"+
                                                "- NONE : Never check schema and use messages without any schema.\n"+
                                                "Default is IGNORE, which usually don't cause any problems but provide estimated schema.";


    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CONNECTION_ID, Type.STRING, null, Importance.HIGH, CONNECTION_ID_DOC)
        .define(BOOTSTRAP_SERVER, Type.STRING, "localhost:9092", Importance.HIGH, "Bootstrap server of response topic. COULD BE DIFFERENT FROM KAFKA CONNECT's BOOTSTRAPs.")
        .define(REQUEST_TOPIC_SUFFIX, Type.STRING, null, Importance.MEDIUM, "The suffix of request topic")
        .define(RESPONSE_TOPIC_SUFFIX, Type.STRING, null, Importance.MEDIUM, "The suffix of response topic")
        .define(CONSUMER_SUFFIX, Type.STRING, null, Importance.MEDIUM, "The suffix of consumer id")
        .define(CONSUMER_GROUP_SUFFIX, Type.STRING, null, Importance.MEDIUM, "The suffix of consumer group")
        .define(SCHEMAPOLICY_WEBREQUEST, Type.STRING, "IGNORE", Importance.LOW, "Schema Policy of web request. "+SCHEMAPOLICY_DOC)
        .define(SCHEMAPOLICY_KAFKAREQUEST, Type.STRING, "IGNORE", Importance.LOW, "Schema Policy of kafka request. "+SCHEMAPOLICY_DOC)
        .define(SCHEMAPOLICY_KAFKARESPONSE, Type.STRING, "IGNORE", Importance.LOW, "Schema Policy of kafka response. "+SCHEMAPOLICY_DOC)
        .define(LOADBALANCER_SCORING, Type.STRING, null, Importance.LOW, LOADBALANCER_SCORING_DOC);

    private String connectionId;
    private String lbScoringMethod;
    private String webRequestSchemaPolicy;
    private String kafkaRequestSchemaPolicy;
    private String kafkaResponseSchemaPolicy;
    private JsonConverter converter;
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        this.connectionId = parsedConfig.getString(CONNECTION_ID);
        this.lbScoringMethod = parsedConfig.getString(LOADBALANCER_SCORING);
        this.webRequestSchemaPolicy = parsedConfig.getString(SCHEMAPOLICY_WEBREQUEST);
        this.kafkaRequestSchemaPolicy = parsedConfig.getString(SCHEMAPOLICY_KAFKAREQUEST);
        this.kafkaResponseSchemaPolicy = parsedConfig.getString(SCHEMAPOLICY_KAFKARESPONSE);
        //generate converter
        Map<String,Object> converterProps = new HashMap<>();
        converterProps.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.BASE64.name());
        converterProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        converterProps.putAll(parsedConfig.originalsWithPrefix("converter."));
        converter = new JsonConverter();
        converter.configure(converterProps);

        //validate settings
        //schema validation
        try{if(validateSchemaPolicy(kafkaRequestSchemaPolicy))
                throw new ConfigException("Wrong value for "+SCHEMAPOLICY_KAFKAREQUEST+
                    ": Schema policy must be one of ONCE, IGNORE. NONE or you have to provide JSONSchema");}
        catch(JsonProcessingException e){ throw new ConfigException("Wrong value for "+SCHEMAPOLICY_KAFKAREQUEST+
                ": Schema policy must be one of ONCE, IGNORE. NONE or you have to provide JSONSchema");}
        catch(DataException e){ throw new ConfigException("Wrong value for "+SCHEMAPOLICY_KAFKAREQUEST+
                ": Provided schema has syntex error(s)");}

        try{if(validateSchemaPolicy(kafkaResponseSchemaPolicy))
                throw new ConfigException("Wrong value fo r"+SCHEMAPOLICY_KAFKARESPONSE+
                    ": Schema policy must be one of ONCE, IGNORE. NONE or you have to provide JSONSchema");}
        catch(JsonProcessingException e){ throw new ConfigException("Wrong value for "+SCHEMAPOLICY_KAFKARESPONSE+
                ": Schema policy must be one of ONCE, IGNORE. NONE or you have to provide JSONSchema");}
        catch(DataException e){ throw new ConfigException("Wrong value for "+SCHEMAPOLICY_KAFKARESPONSE+
                ": Provided schema has syntex error(s)");}

        try{if(validateSchemaPolicy(webRequestSchemaPolicy))
                throw new ConfigException("Wrong value for "+SCHEMAPOLICY_WEBREQUEST+
                    ": Schema policy must be one of ONCE, IGNORE. NONE or you have to provide JSONSchema");}  
        catch(JsonProcessingException e){ throw new ConfigException("Wrong value for"+SCHEMAPOLICY_WEBREQUEST+
                ": chema policy must be one of ONCE, IGNORE. NONE or you have to provide JSONSchema");}
        catch(DataException e){ throw new ConfigException("Wrong value for"+SCHEMAPOLICY_WEBREQUEST+
                ": Provided schema has syntex error(s)");}

        if (RESTContextManager.getInstance().isRegistered(this.connectionId))
            throw new ConfigException(String.format("ConnectionId %s is already in use.", this.connectionId));

        try{
            //register this
            log.info("registering connector for connection {}...",connectionId);
            RESTContextManager.getInstance().registerConnector(connectionId, parsedConfig);
            //loadbalancer and template will be generated here
        }
        catch(Exception e){
            //if register failed, remove contexts generated by this
            RESTContextManager.getInstance().deregisterConnector(this.connectionId);
            throw new ConfigException(String.format("Connection %s failed due to error",this.connectionId)+e.getMessage());
        }
        
    }

    private boolean validateSchemaPolicy(String schemaPolicy) throws JsonProcessingException{
        List<String> schemaPolicies = Arrays.asList("ONCE","IGNORE","NONE");
        if ((schemaPolicy == null)||(schemaPolicy.equals(""))) return false;
        if (schemaPolicies.contains(schemaPolicy)) return true;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaNode = mapper.readTree(schemaPolicy); //JsonProcessingException could be thrown here
        Schema schema = converter.asConnectSchema(schemaNode); //DataException could be thrown here
        return schema != null;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RESTInputSourceTask.class;
    }

    public static final String TASK_INDEX = "synk.task.id";
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        //#####################################################
        // ##### IMPORTANT PART OF KAFKA CONNECT RUNTIME #####
        //#####################################################
        //NOTE the number of tasks is not determined by maxTasks value in config, but the size of the list we return in this function.
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        //we'll generate tasks by number of maxTasks.
        for(int i=0; i<maxTasks; i++){
            Map<String,String> config = new HashMap<>();
            config.put(TASK_INDEX, String.valueOf(i)); //task id for log
            config.put(CONNECTION_ID, connectionId); //They might have to refer the registry
            config.put(LOADBALANCER_SCORING, lbScoringMethod); //Loadbalancer scoring method (by number? by bytes?)
            configs.add(config);
        }
        return configs;
    }
    

    @Override
    public void stop() {
        //deregister this from Manager
        RESTContextManager.getInstance().deregisterConnector(this.connectionId);
        log.trace("Connector for connection {} is deregistered",this.connectionId);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    
}
