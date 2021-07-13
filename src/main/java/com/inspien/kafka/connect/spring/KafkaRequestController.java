package com.inspien.kafka.connect.spring;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.json.JsonParseException;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

/**
 * mainly controlls all connect act in here.
 * 
 */
@Slf4j
@RestController
public class KafkaRequestController {

	@Autowired
	ReplyingKafkaTemplate<String, SourceRecord, SinkRecord> kafkaTemplate;
	
	@Value("${kafka.topic.request-topic}")
	String requestTopic;
	
	private String webRequestSchemaPolicy;
	private String kafkaRequestSchemaPolicy;
	private String kafkaResponseSchemaPolicy;

	public void setWebRequestSchemaPolicy(@Value("${synk.schema.web-request:ONCE") String policy){
		this.webRequestSchemaPolicy = policy;
	}
	
	public void setKafkaRequestSchemaPolicy(@Value("${synk.schema.kafka-request:ONCE") String policy){
		this.kafkaRequestSchemaPolicy = policy;
	}
	
	public void setKafkaResponseSchemaPolicy(@Value("${synk.schema.kafka-response:ONCE") String policy){
		this.kafkaResponseSchemaPolicy = policy;
	}

	@Value("${kafka.topic.requestreply-topic}")
	String requestReplyTopic;

	@ResponseBody
	@PostMapping(value="${kafka.topic.requestreply-topic}",produces=MediaType.APPLICATION_JSON_VALUE,consumes=MediaType.APPLICATION_JSON_VALUE)
	public String request(@RequestBody String request) throws JsonParseException, InterruptedException, ExecutionException {
		RESTConfig config = new RESTConfig();
		
        ArrayList<SourceRecord> records = new ArrayList<>();
        //parse request
		JsonNode jsonbody;
		try{
			ObjectMapper mapper = new ObjectMapper();
			jsonbody = mapper.readTree(request);
			log.trace("successfully parsed request : {}",request);
		}
		catch(JsonProcessingException e){
			throw new JsonParseException(e);
		}
		//generate Connectrecord from body as input of transform chain
        //records.add(new SourceRecord())
        return "";
	}

	private boolean checkSchema(JsonNode message, Schema schema){
		return message.isArray();
	}


	/**
	 * Generate Kafka Connect Schema from input JSON. Used in schema autogeneration and checking.
	 * Schemas could be delivered via configs, or Schema Registry
	 * @param json root node of json message
	 * @return
	 */
	@Bean
	public Schema buildSchemaFromJSON(JsonNode json){
		if (json==null){
			log.warn("While autogenerating schema, null is detected. Cannot assume schema so string is taken");
			return Schema.STRING_SCHEMA;
		}

		switch (json.getNodeType()){
			//if array, get first item and generate array schema
			case ARRAY:
				//get schema of child
				if (json.size()>0){
					Schema childSchema;
					childSchema = buildSchemaFromJSON(json.get(0));
					if (childSchema == null){
						log.warn("While autogenerating schema, null is detected. Cannot assume schema so string is taken");
						return Schema.STRING_SCHEMA;
					}
					return SchemaBuilder.array(childSchema).build();
				}
				log.warn("While autogenerating schema, null is detected. Cannot assume schema so string is taken");
				return Schema.STRING_SCHEMA;
			case BOOLEAN:
				return Schema.BOOLEAN_SCHEMA;
			case NULL:
				log.warn("While autogenerating schema, null is detected. Cannot assume schema so string is taken");
				return Schema.STRING_SCHEMA;
			case NUMBER:
				//parse number and return appropriate number schema
				if(json.toString().contains(".")) return Schema.FLOAT64_SCHEMA;
				return Schema.INT64_SCHEMA;
			case OBJECT:
				//if object, return subschema
				SchemaBuilder childSchemaBuilder = SchemaBuilder.struct();
				json.fieldNames()
				    .forEachRemaining(
						fname -> childSchemaBuilder.field(fname,buildSchemaFromJSON(json.get(fname))));
				return childSchemaBuilder.build();
			case STRING:
				return Schema.STRING_SCHEMA;
			default:
				log.warn("While autogenerating schema, cannot assign field '{}' to any schema so string is taken",json.toString());
				return Schema.STRING_SCHEMA;
		}
	}
}