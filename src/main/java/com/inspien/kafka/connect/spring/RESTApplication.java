package com.inspien.kafka.connect.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

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
	}

}
