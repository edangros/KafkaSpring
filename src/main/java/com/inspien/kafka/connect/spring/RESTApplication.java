package com.inspien.kafka.connect.spring;



import com.inspien.kafka.connect.RESTContextRegistry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
/**
 * Launch Spring REST Endpoint using Config settings. 
 */
public class RESTApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(RESTApplication.class, args);
		log.trace("WebService {} is launched", context.getApplicationName());
	}

}
