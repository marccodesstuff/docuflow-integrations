package com.docuflow.integrations.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class NotificationRoute extends RouteBuilder {

    @Value("${docuflow.integrations.email.enabled:false}")
    private boolean emailEnabled;

    @Value("${docuflow.integrations.email.smtp-host:}")
    private String smtpHost;

    @Value("${docuflow.integrations.email.smtp-port:587}")
    private int smtpPort;

    @Value("${docuflow.integrations.email.username:}")
    private String emailUsername;

    @Value("${docuflow.integrations.email.password:}")
    private String emailPassword;

    @Value("${docuflow.integrations.email.from-address:noreply@docuflow.io}")
    private String fromAddress;

    @Value("${docuflow.integrations.email.use-tls:true}")
    private boolean useTls;

    @Value("${docuflow.integrations.sms.enabled:false}")
    private boolean smsEnabled;

    @Value("${docuflow.integrations.sms.provider:twilio}")
    private String smsProvider;

    @Override
    public void configure() {
        // Email notification route
        if (emailEnabled) {
            from("kafka:docuflow.notifications.email?groupId=docuflow-integrations&autoOffsetReset=earliest")
                .routeId("email-notification")
                .process(exchange -> {
                    String to = exchange.getIn().getHeader("to", String.class);
                    String subject = exchange.getIn().getHeader("subject", String.class);
                    String body = exchange.getIn().getBody(String.class);
                    
                    exchange.getIn().setHeader("To", to);
                    exchange.getIn().setHeader("Subject", subject);
                    exchange.getIn().setHeader("From", fromAddress);
                    exchange.getIn().setBody(body);
                })
                .to("smtp://${smtpHost}:${smtpPort}?username=${emailUsername}&password=${emailPassword}&useTLS=${useTls}")
                .choice()
                    .when(simple("${header.CamelSmtpStatus} == 'SUCCESS'"))
                        .log("Email sent successfully to ${header.To}")
                    .otherwise()
                        .log("Email failed to ${header.To}: ${header.CamelSmtpErrorMessage}")
                        .to("rabbitmq:docuflow.notifications.retry")
                .endChoice();
        }

        // SMS notification route
        if (smsEnabled) {
            from("kafka:docuflow.notifications.sms?groupId=docuflow-integrations&autoOffsetReset=earliest")
                .routeId("sms-notification")
                .choice()
                    .when(simple("${header.smsProvider} == 'twilio'"))
                        .to("direct:send-twilio-sms")
                    .when(simple("${header.smsProvider} == 'vonage'"))
                        .to("direct:send-vonage-sms")
                    .otherwise()
                        .log("Unknown SMS provider: ${header.smsProvider}")
                .endChoice();
        }

        // Twilio SMS
        if (smsEnabled && "twilio".equals(smsProvider)) {
            from("direct:send-twilio-sms")
                .routeId("send-twilio-sms")
                .process(exchange -> {
                    String to = exchange.getIn().getHeader("to", String.class);
                    String body = exchange.getIn().getBody(String.class);
                    String from = exchange.getIn().getHeader("from", String.class);
                    
                    exchange.getIn().setHeader("CamelHttpMethod", constant("POST"));
                    exchange.getIn().setHeader("Content-Type", constant("application/x-www-form-urlencoded"));
                    exchange.getIn().setBody("To=" + to + "&From=" + from + "&Body=" + body);
                })
                .to("https4://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Messages.json?authUsername=${TWILIO_ACCOUNT_SID}&authPassword=${TWILIO_AUTH_TOKEN}")
                .choice()
                    .when(simple("${header.CamelHttpResponseCode} == 201"))
                        .log("SMS sent via Twilio to ${header.to}")
                    .otherwise()
                        .log("Twilio SMS failed: ${header.CamelHttpResponseCode}")
                        .to("rabbitmq:docuflow.notifications.retry")
                .endChoice();
        }

        // Vonage SMS
        if (smsEnabled && "vonage".equals(smsProvider)) {
            from("direct:send-vonage-sms")
                .routeId("send-vonage-sms")
                .process(exchange -> {
                    String to = exchange.getIn().getHeader("to", String.class);
                    String body = exchange.getIn().getBody(String.class);
                    String from = exchange.getIn().getHeader("from", String.class);
                    
                    exchange.getIn().setHeader("CamelHttpMethod", constant("POST"));
                    exchange.getIn().setHeader("Content-Type", constant("application/json"));
                    exchange.getIn().setBody("{\"from\":\"" + from + "\",\"to\":\"" + to + "\",\"text\":\"" + body + "\"}");
                })
                .to("https4://rest.nexmo.com/sms/json?authUsername=${VONAGE_API_KEY}&authPassword=${VONAGE_API_SECRET}")
                .choice()
                    .when(simple("${header.CamelHttpResponseCode} == 200"))
                        .log("SMS sent via Vonage to ${header.to}")
                    .otherwise()
                        .log("Vonage SMS failed: ${header.CamelHttpResponseCode}")
                        .to("rabbitmq:docuflow.notifications.retry")
                .endChoice();
        }

        // Notification retry route
        from("rabbitmq:docuflow.notifications.retry?queue=docuflow.notifications.retry&autoDelete=false&durable=true")
            .routeId("notification-retry")
            .delay(simple("${header.retryDelay}"))
            .choice()
                .when(simple("${header.notificationType} == 'email'"))
                    .to("kafka:docuflow.notifications.email")
                .when(simple("${header.notificationType} == 'sms'"))
                    .to("kafka:docuflow.notifications.sms")
                .otherwise()
                    .log("Unknown notification type for retry")
            .endChoice();
    }
}