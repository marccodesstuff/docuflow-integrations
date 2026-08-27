package com.docuflow.integrations.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WebhookDeliveryRoute extends RouteBuilder {

    @Value("${docuflow.integrations.webhook.max-retries:5}")
    private int maxRetries;

    @Value("${docuflow.integrations.webhook.retry-delays-seconds:[60, 300, 900, 3600, 21600]}")
    private int[] retryDelays;

    @Value("${docuflow.integrations.webhook.timeout-seconds:10}")
    private int timeoutSeconds;

    @Value("${docuflow.integrations.webhook.signature-header:X-DocuFlow-Signature}")
    private String signatureHeader;

    @Override
    public void configure() {
        // Dead letter queue for failed deliveries
        errorHandler(deadLetterChannel("rabbitmq:docuflow.webhook.dlq")
            .maximumRedeliveries(maxRetries)
            .redeliveryDelay(retryDelays[0])
            .asyncDelayedRedelivery(true)
            .retryAttemptedLogLevel(org.apache.camel.LoggingLevel.WARN));

        // Webhook delivery route - consumes from RabbitMQ queue
        from("rabbitmq:docuflow.webhook.deliveries?queue=docuflow.webhook.deliveries&autoDelete=false&durable=true")
            .routeId("webhook-delivery")
            .setHeader("CamelHttpMethod", constant("POST"))
            .setHeader("Content-Type", constant("application/json"))
            .setHeader(signatureHeader, simple("${body}")) // Would compute HMAC in processor
            .process(exchange -> {
                // Add idempotency key
                String deliveryId = exchange.getIn().getHeader("deliveryId", String.class);
                exchange.getIn().setHeader("X-DocuFlow-Delivery-ID", deliveryId);
            })
            .toD("http:${header.webhookUrl}?connectTimeout=${timeoutSeconds}000&socketTimeout=${timeoutSeconds}000&throwExceptionOnFailure=false")
            .choice()
                .when(simple("${header.CamelHttpResponseCode} >= 200 && ${header.CamelHttpResponseCode} < 300"))
                    .log("Webhook delivered successfully to ${header.webhookUrl}")
                    .process(exchange -> {
                        // Mark delivery as successful in database
                    })
                .otherwise()
                    .log("Webhook delivery failed with status ${header.CamelHttpResponseCode} to ${header.webhookUrl}")
                    .throwException(new RuntimeException("Webhook delivery failed: HTTP ${header.CamelHttpResponseCode}"))
            .endChoice();

        // Retry route with exponential backoff
        from("rabbitmq:docuflow.webhook.retry?queue=docuflow.webhook.retry&autoDelete=false&durable=true")
            .routeId("webhook-retry")
            .process(exchange -> {
                int attempt = exchange.getIn().getHeader("attempt", Integer.class);
                if (attempt < maxRetries && attempt < retryDelays.length) {
                    long delay = retryDelays[attempt] * 1000L;
                    exchange.getIn().setHeader("delay", delay);
                } else {
                    // Max retries exceeded, send to DLQ
                    exchange.getIn().setHeader("maxRetriesExceeded", true);
                }
            })
            .choice()
                .when(simple("${header.maxRetriesExceeded}"))
                    .to("rabbitmq:docuflow.webhook.dlq")
                .otherwise()
                    .delay(simple("${header.delay}"))
                    .to("rabbitmq:docuflow.webhook.deliveries")
            .endChoice();
    }
}