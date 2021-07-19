package com.inspien.kafka.connect;


import static java.lang.System.currentTimeMillis;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inspien.kafka.connect.RESTContextManager.SchemaType;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {
    
    private Utils(){}

	public static final JsonConverter CONVERTER = new JsonConverter();
	public static final ObjectMapper MAPPER = new ObjectMapper();
	public static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);
	static {
		final Map<String,Object> converterporps = new HashMap<>();
		converterporps.put(ConverterConfig.TYPE_CONFIG, "value");
		converterporps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
		CONVERTER.configure(converterporps);
	}
    /**
     * Check if the port is in use
     * @param port target port number
     * @return if port is accessable, return {@code true}. Return {@code false} if the port is in use or locked.
     */
    public static boolean validatePortNumber(int port){
        try (ServerSocket serverSocket = new ServerSocket()) {
            // setReuseAddress(false) is required only on OSX, 
            // otherwise the code will not work correctly on that platform          
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
        } catch (Exception ex) {
            return false;
        }
		return true;
    }
	/**
	 * get local ip, which will be used as partition
	 * @return local ip address, or loopback address 127.0.0.1 if can't gain any address
	 */
	public static String getLocalIp(){
		String ip;
		try{
			ip = InetAddress.getLocalHost().getHostAddress();
		}
		catch (UnknownHostException e){
			log.error("cannot find localhost maybe due to security setting. localhost 127.0.0.1 is used",e);
			ip = "127.0.0.1";
		}
		return ip;
	}

	
	/**
	 * Generate Kafka Connect Schema from input JSON. Used in schema autogeneration and checking.
	 * Schemas could be delivered via configs, or Schema Registry if optional (Currently not supported)
	 * @param json root node of json message
	 * @return
	 */
	public static Schema buildSchemaFromJSON(JsonNode json){
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
	
	/**
	 * Convert JSON request to Connect Record. This follows the logic of {@link JsonConverter} and {@link Transformation}.
	 * In most cases, Schema should be provided for further transformation and validation, so this generates schema from
	 * message even if there are no provided schema.
	 * @param json json message
	 * @param registryId {@link RESTContextManager} key of the connection where schema is stored.
	 * @return {@link SourceRecord} generated from JSON request, which partition is uri of worker and offset is timestamp.
	 */
	public static SourceRecord convertJSONtoConnectRecord(JsonNode json, String registryId){

		log.info("{} : Convert {} to Connect data",registryId, json);
		JsonNode msgSchema = null;

		//check if Schema-Payload type(Kafka Connect Message type) request arrived
		if ( (json.has("schema") && json.has("payload")) ){
			//split message by msgschema and jsonbody
			msgSchema = json.get("schema");
			json = json.get("payload");
		}

		//get config from registry
		Schema schema = RESTContextManager.getInstance().getSchema(registryId, SchemaType.REQUEST);

		switch(RESTContextManager.getInstance().getConfigFieldString(registryId, RESTSyncConnector.SCHEMAPOLICY_WEBREQUEST)){
			//Generate or gain Schema if schema not exist.
			case "ONCE":
				if (schema == null){
					if (msgSchema == null) schema = Utils.buildSchemaFromJSON(json);
					else schema = CONVERTER.asConnectSchema(msgSchema);
					RESTContextManager.getInstance().registerRequestSchema(registryId, schema);
				}
				else{
					if (msgSchema != null){
						log.warn("SchemaPolicy is set to 'ONCE' and schema is already set, but received new schema. Provided schema is ignored.");
					}
				}
				break;
			//Ignore schema but attach schema for further process
			case "IGNORE":
				if (msgSchema != null) schema = CONVERTER.asConnectSchema(msgSchema);
				else schema = Utils.buildSchemaFromJSON(json);
				break;
			//do not attach schema
			case "NONE":
				schema = null;
				break;
			default:
		}

		//treat schema
		if (schema == null) return convertJSONSchemaless(json, registryId);
		else return convertJSONWithSchema(json, schema, registryId);
	}


	/**
	 * Convert JSON Message without any schema. Message will be treated as plain text so need to be parased at opponents.
	 * @param json JSON message
	 * @param registryId {@link RESTContextManager} key of the connection, where topic name is stored.
	 * @return converted JSON message.
	 */
	private static SourceRecord convertJSONSchemaless(JsonNode json, String registryId){
		String topic = RESTContextManager.getInstance().getConfigFieldString(registryId, RESTSyncConnector.CONNECTION_ID)+
						RESTContextManager.getInstance().getConfigFieldString(registryId, RESTSyncConnector.REQUEST_TOPIC_SUFFIX);
		SchemaAndValue value = CONVERTER.toConnectData(topic, json.toString().getBytes());
		//now we have schema
		return new SourceRecord(
			Collections.singletonMap("Sender", Utils.getLocalIp()),//partition is uri
			Collections.singletonMap("time", currentTimeMillis()), //Timestamp could be best as offset if all server time is syncronized.
			topic,
			value.schema(), //use provided schema
			value.value()
		);
	}


	/**
	 * Convert JSON Message with provided {@link Schema}.
	 * @param json JSON message
	 * @param schema Connect {@link Schema} of the message. If message is not fit to schema, {@link DataException}will be thrown
	 * @param registryId {@link RESTContextManager} key of the connection, where topic name is stored.
	 * @return converted JSON message.
	 * @throws DataException If provided message is not fit to schema. internally uses {@link ConnectSchema}'s validation method.
	 */
	private static SourceRecord convertJSONWithSchema(JsonNode json, Schema schema, String registryId) throws DataException{
		String topic = RESTContextManager.getInstance().getConfigFieldString(registryId, RESTSyncConnector.CONNECTION_ID)+
						RESTContextManager.getInstance().getConfigFieldString(registryId, RESTSyncConnector.REQUEST_TOPIC_SUFFIX);
		//generate value as struct
		ObjectNode root = NODE_FACTORY.objectNode();
		root.replace("payload", json);
		root.replace("schema", CONVERTER.asJsonSchema(schema));
		SchemaAndValue value = CONVERTER.toConnectData(topic, root.toString().getBytes());

		//validate schema
		ConnectSchema cSchema = (ConnectSchema)schema;
		// TODO Connectschema not supports validation. Implementation of field-by-field validation is needed.
		// Maybe There are good sources in Kafka Connect Transformation or Schema Registry.
		// cSchema.validateValue(value.value());
		//now we have schema
		return new SourceRecord(
			Collections.singletonMap("Sender", Utils.getLocalIp()),//partition is uri
			Collections.singletonMap("time", currentTimeMillis()),//Timestamp could be best for offset
			topic,
			value.schema(), //use provided schema
			value.value()
		);
	}
}
