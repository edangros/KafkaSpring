package com.inspien.kafka.connect.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.inspien.kafka.connect.RESTContextManager;
import com.inspien.kafka.connect.ReplyingKafkaConnectTemplate;
import com.inspien.kafka.connect.Utils;
import com.inspien.kafka.connect.RESTContextManager.SchemaType;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.json.JsonParseException;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.concurrent.SettableListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;


@Controller
@Slf4j
public class RESTController {
    
	@Autowired
	Environment environment;
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
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Request message is not fit to Schema : \n"+
														Utils.CONVERTER.asJsonSchema(RESTContextManager.getInstance().getSchema(connectionId,SchemaType.REQUEST))+"\n"+
														e.toString());
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
		String body = generateResultMessageBody(futures, requestRecords);
        //records.add(new SourceRecord())
        return ResponseEntity.ok().body(body);
	}

	private String generateResultMessageBody(List<SettableListenableFuture<SinkRecord>> futures, List<SourceRecord> requestRecords) throws JsonParseException, InterruptedException, ExecutionException {
		//generate message body
		String body = "";
		//if single message, no list wraps that, just return
		if(futures.size() == 1){
			//TODO Connect JsonConverter is not able to generate proper JSON message - have to build it(...)
			/*
			try{
				JsonNode requestNode = Utils.MAPPER.readTree(requestRecords.get(0).value().toString());
				JsonNode responseNode = Utils.MAPPER.readTree(new String(
					Utils.CONVERTER.fromConnectData("",futures.get(0).get().valueSchema(),futures.get(0).get().value())));
				body = pairRequestAndResponse(requestNode, responseNode).toString();
			}
			catch(JsonProcessingException e){
				log.error("Can't handle Response message : {} \n Cause {}", futures.get(0).get().toString(),e.toString());
				body = "Can't handle Response message : "+ futures.get(0).get().toString() +" \n Cause "+ e.toString();
			}
			*/
			body = futures.get(0).get().value().toString();
		}
		//if listed message, return as list
		else{
			ArrayNode rootNode = Utils.NODE_FACTORY.arrayNode();
			/*
			for(int i = 0;i<futures.size();i++){
				try{
					JsonNode requestNode = Utils.MAPPER.readTree(requestRecords.get(i).value().toString());
					JsonNode responseNode = Utils.MAPPER.readTree(new String(
						Utils.CONVERTER.fromConnectData("",futures.get(i).get().valueSchema(),futures.get(i).get().value())));
					rootNode.add(pairRequestAndResponse(requestNode, responseNode));
					rootNode.add(Utils.NODE_FACTORY.textNode(futures.get(i).get().toString()));
				}
				catch(JsonProcessingException e){
					log.error("Can't handle Response message : ", futures.get(i).get().toString());
					//treat string:string
					TextNode requestNode = Utils.NODE_FACTORY.textNode(requestRecords.get(i).toString());
					TextNode responseNode = Utils.NODE_FACTORY.textNode(
						new String (Utils.CONVERTER.fromConnectData("", futures.get(i).get().valueSchema(), futures.get(i).get().value())));
					rootNode.add(pairRequestAndResponse(requestNode, responseNode));
				}
			}
			*/
			body = rootNode.toString();
		}
		return body;
	}
	/**
	 * Generate Request-Response pair, which will be used in HTTP response body.
	 * @param request request JSON message
	 * @param response response JSON message
	 * @return {"request":_request, "response":_response} style json message
	 */
	private JsonNode pairRequestAndResponse(JsonNode request, JsonNode response){
		ObjectNode node = Utils.NODE_FACTORY.objectNode();
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
		jsonbody = Utils.MAPPER.readTree(request);
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
			records.add(Utils.convertJSONtoConnectRecord(msg, registryId));
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
					sb.append("[error] : "+e.toString()+"\n\n");
				}
				catch(CancellationException e){
					log.error("a task is Canceled : ", e);
					canceledWithError += 1;
					sb.append("[error] : "+e.toString()+"\n\n");
				}
				catch(InterruptedException e){
					canceledWithError += 1;
					sb.append("[error] : "+e.toString()+"\n\n");
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

}
