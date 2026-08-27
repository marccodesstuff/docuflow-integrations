package com.docuflow.integrations;

import org.apache.camel.spring.boot.CamelSpringBootApplicationController;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.docuflow.integrations")
@EnableAsync
@EnableScheduling
public class DocuFlowIntegrationsApplication {

    public static void main(String[] args) {
        SpringApplication.run(DocuFlowIntegrationsApplication.class, args);
    }
}