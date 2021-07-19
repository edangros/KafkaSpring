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
import org.springframework.util.concurrent.SettableListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
/**
 * Launch Spring REST Endpoint using Config settings. 
 * Web Applicatiion and tasks are decoupled from each other.
 * Web application finds task contexts from {@link RESTContextManager}, then find task from {@link TaskLoadBalancer.}
 */
public class RESTApplication  implements CommandLineRunner{

	@Autowired
	Environment environment;

	@Override
    public void run(String... args) throws Exception {
        main(args);
    }

	public static void main(String[] args) {
		log.info("web server Main method started-completed configuration.");
		//ConfigurableApplicationContext context = SpringApplication.run(RESTApplication.class, args);
		//log.trace("WebService {} is launched at port ",context.getApplicationName(), context.getEnvironment().getProperty("local.server.port"));
	}

}
