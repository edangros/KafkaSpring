package com.inspien.kafka.connect.spring;



import static java.lang.System.currentTimeMillis;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonpCharacterEscapes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.inspien.kafka.connect.RESTContextManager;
import com.inspien.kafka.connect.RESTSyncConnector;
import com.inspien.kafka.connect.Utils;
import com.inspien.kafka.connect.RESTContextManager.SchemaType;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.json.JsonParseException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.SettableListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication

@ComponentScan(basePackages = {"com.inspien.kafka.connect.spring"})  //scan only spring api package to prevent multiple singleton instances exist
/**
 * Launch Spring REST Endpoint using Config settings. 
 * Web Applicatiion and tasks are decoupled from each other.
 * Web application finds task contexts from {@link RESTContextManager}, then find task from {@link TaskLoadBalancer.}
 */
public class RESTApplication {

	@Autowired
	Environment environment;
	private static final JsonConverter CONVERTER = new JsonConverter();
	private static ObjectMapper MAPPER = new ObjectMapper();
	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(RESTApplication.class, args);
		log.trace("WebService {} is launched at port ",context.getApplicationName(), context.getEnvironment().getProperty("local.server.port"));
	}

	private static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);

	/**
	 * Main request process. Uses {@link ReplyingKafkaConnectTemplate} to treat requests.
	 * @param connectionId connection ID, provided in uri path
	 * @param request request body.
	 * @return response message, will be sent to requester
	 * @throws JsonParseException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	@ResponseBody
	@PostMapping(value="/connection/{id}",produces=MediaType.APPLICATION_JSON_VALUE,consumes=MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> request(@PathVariable("id") String connectionId, @RequestBody String request) throws JsonParseException, InterruptedException, ExecutionException {
		if (!RESTContextManager.getInstance().isRegistered(connectionId))
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(String.format("Connection %s is not found",connectionId));
		List<SourceRecord> requestRecords;
		try{
			requestRecords = parseRequest(request, connectionId);
		}
		catch(JsonProcessingException e){
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Request body must be JSON");
		}
		catch(DataException e){
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Request message is not fit to Schema :%n"+
														CONVERTER.asJsonSchema(RESTContextManager.getInstance().getSchema(connectionId,SchemaType.REQUEST)));
		}
		//Obtain Template
		ReplyingKafkaConnectTemplate template = RESTContextManager.getInstance().getTemplate(connectionId);

		
		List<SettableListenableFuture<SinkRecord>> futures = new ArrayList<>();
		List<CompletableFuture<?>> completables = new ArrayList<>();
		for(SourceRecord record : requestRecords){
			SettableListenableFuture<SinkRecord> future = template.sendAndReceive(record);
			futures.add(future);
			completables.add(future.completable());
		}

		//wait for all futures are complete
		CompletableFuture<?>[] futuresToWait = new CompletableFuture<?>[completables.size()];
		completables.toArray(futuresToWait);

		try{
			CompletableFuture.allOf(futuresToWait).join();
		}
		catch(CancellationException |CompletionException e){
			log.error("Request procedure was cancelled due to error(s) : ", e);
			abortAndGenerateResponseBody(futures);
		}
		

		//now all futures are set
		List<SinkRecord> replies = new ArrayList<>();
		for (SettableListenableFuture<SinkRecord> record : futures){
			replies.add(record.get());
		}
		
		//generate message body
		String body = "";
		//if single message, no list wraps that, just return
		if(futures.size() == 1){
			try{
				JsonNode requestNode = MAPPER.readTree(requestRecords.get(0).value().toString());
				JsonNode responseNode = MAPPER.readTree(futures.get(0).get().toString());
				body = pairRequestAndResponse(requestNode, responseNode).toString();
			}
			catch(JsonProcessingException e){
				log.error("Can't handle Response message : {}", futures.get(0).get().toString());
				body = "Can't handle Response message :"+ futures.get(0).get().toString();
			}
		}
		//if listed message, return as list
		else{
			ArrayNode rootNode = NODE_FACTORY.arrayNode();
			for(int i = 0;i<futures.size();i++){
				try{
					JsonNode requestNode = MAPPER.readTree(requestRecords.get(i).value().toString());
					JsonNode responseNode = MAPPER.readTree(futures.get(i).get().toString());
					rootNode.add(pairRequestAndResponse(requestNode, responseNode));
				}
				catch(JsonProcessingException e){
					log.error("Can't handle Response message : ", futures.get(i).get().toString());
					//treat string:string
					TextNode requestNode = NODE_FACTORY.textNode(requestRecords.get(i).toString());
					TextNode responseNode = NODE_FACTORY.textNode(futures.get(i).get().toString());
					rootNode.add(pairRequestAndResponse(requestNode, responseNode));
				}
			}
			body = rootNode.toString();
		}
        //records.add(new SourceRecord())
        return ResponseEntity.ok().body(body);
	}

	/**
	 * Generate Request-Response pair, which will be used in HTTP response body.
	 * @param request request JSON message
	 * @param response response JSON message
	 * @return {"request":_request, "response":_response} style json message
	 */
	private JsonNode pairRequestAndResponse(JsonNode request, JsonNode response){
		ObjectNode node = NODE_FACTORY.objectNode();
		node.replace("request", request);
		node.replace("response", response);
		return node;
	}

	/**
	 * Parse request JSON message. Internally, this process uses Connect's {@link JsonConverter} to convert message with or w/o schema.
	 * @param request
	 * @param registryId
	 * @return
	 * @throws JsonProcessingException
	 */
	public List<SourceRecord> parseRequest(String request, String registryId) throws JsonProcessingException{
		//parse request
		JsonNode jsonbody;
		jsonbody = MAPPER.readTree(request);
		log.trace("successfully parsed request : {}",request);
		List<JsonNode> messages = new ArrayList<>();
		//check if multiple messages arrived
		if (jsonbody.isArray()){
			for(final JsonNode node : jsonbody){
				messages.add(node);
			}
		}
		else {
			messages.add(jsonbody);
		}
		List<SourceRecord> records = new ArrayList<>();
		for (JsonNode msg : messages){
			records.add(convertJSONtoConnectRecord(msg, registryId));
		}
		return records;
	}

	/**
	 * Abort all waiting task for this request and retrives any messages sent.
	 * If there are error in any message, requester will not get expected result anyway so there are no needs to wait any response.
	 * This will abort any listening task associated with the request and retrives any response arrived then generate body message to requester.
	 * @param responseFutures responsefutures which generated by the request
	 * @return body message as string, with reports of message treated.
	 */
	public String abortAndGenerateResponseBody(List<SettableListenableFuture<SinkRecord>> responseFutures){
		StringBuilder sb = new StringBuilder();
		int canceledWithError = 0;
		int aborted = 0;
		int received = 0;
		for(SettableListenableFuture<SinkRecord> future : responseFutures){
			if (future.isDone()){
				try{
					sb.append("[retrived] : ");
					sb.append(future.get().toString());
					sb.append("%n");
					received += 1;
				}
				catch(ExecutionException e){
					//ensure future thread is killed
					future.cancel(true);
					canceledWithError += 1;
					sb.append("[error] : "+e.toString()+"%n%n");
				}
				catch(CancellationException e){
					log.error("a task is Canceled : ", e);
					canceledWithError += 1;
					sb.append("[error] : "+e.toString()+"%n%n");
				}
				catch(InterruptedException e){
					canceledWithError += 1;
					sb.append("[error] : "+e.toString()+"%n%n");
					Thread.currentThread().interrupt();
				}
			}
			else{
				//we have to abort - there is an error
				aborted += 1;
				future.cancel(true);
			}
		}
		String report = String.format("[aborted] : %d%n[error] : %d%n[received]%d%n%n%n",aborted,canceledWithError,received);
		return report+sb.toString();
	}

	/**
	 * Convert JSON request to Connect Record. This follows the logic of {@link JsonConverter} and {@link Transformation}.
	 * In most cases, Schema should be provided for further transformation and validation, so this generates schema from
	 * message even if there are no provided schema.
	 * @param json json message
	 * @param registryId {@link RESTContextManager} key of the connection where schema is stored.
	 * @return {@link SourceRecord} generated from JSON request, which partition is uri of worker and offset is timestamp.
	 */
	public SourceRecord convertJSONtoConnectRecord(JsonNode json, String registryId){

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
					if (msgSchema == null) schema = buildSchemaFromJSON(json);
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
				if (msgSchema == null) schema = CONVERTER.asConnectSchema(msgSchema);
				else schema = buildSchemaFromJSON(json);
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
	private SourceRecord convertJSONSchemaless(JsonNode json, String registryId){
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
	private SourceRecord convertJSONWithSchema(JsonNode json, Schema schema, String registryId) throws DataException{
		String topic = RESTContextManager.getInstance().getConfigFieldString(registryId, RESTSyncConnector.CONNECTION_ID)+
						RESTContextManager.getInstance().getConfigFieldString(registryId, RESTSyncConnector.REQUEST_TOPIC_SUFFIX);
		//generate value as struct
		ObjectNode root = NODE_FACTORY.objectNode();
		root.replace("payload", json);
		root.replace("schema", CONVERTER.asJsonSchema(schema));
		SchemaAndValue value = CONVERTER.toConnectData(topic, root.toString().getBytes());

		//validate schema
		ConnectSchema cSchema = (ConnectSchema)schema;
		cSchema.validateValue(value.value());
		//now we have schema
		return new SourceRecord(
			Collections.singletonMap("Sender", Utils.getLocalIp()),//partition is uri
			Collections.singletonMap("time", currentTimeMillis()),//Timestamp could be best for offset
			topic,
			value.schema(), //use provided schema
			value.value()
		);
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
}
