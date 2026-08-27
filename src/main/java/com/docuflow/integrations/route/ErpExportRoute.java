package com.docuflow.integrations.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ErpExportRoute extends RouteBuilder {

    @Value("${docuflow.integrations.erp.sap.enabled:false}")
    private boolean sapEnabled;

    @Value("${docuflow.integrations.erp.netsuite.enabled:false}")
    private boolean netsuiteEnabled;

    @Value("${docuflow.integrations.erp.odoo.enabled:false}")
    private boolean odooEnabled;

    @Override
    public void configure() {
        // Generic ERP export route - consumes from Kafka topic
        from("kafka:docuflow.exports.erp?groupId=docuflow-integrations&autoOffsetReset=earliest")
            .routeId("erp-export")
            .process(exchange -> {
                String erpSystem = exchange.getIn().getHeader("erpSystem", String.class);
                exchange.getIn().setHeader("targetErp", erpSystem);
            })
            .choice()
                .when(simple("${header.targetErp} == 'sap'"))
                    .to("direct:export-sap")
                .when(simple("${header.targetErp} == 'netsuite'"))
                    .to("direct:export-netsuite")
                .when(simple("${header.targetErp} == 'odoo'"))
                    .to("direct:export-odoo")
                .otherwise()
                    .log("Unknown ERP system: ${header.targetErp}")
                    .to("rabbitmq:docuflow.exports.dlq")
            .endChoice();

        // SAP Export Route
        if (sapEnabled) {
            from("direct:export-sap")
                .routeId("export-sap")
                .process(exchange -> {
                    // Transform to SAP BAPI/IDoc format
                    // Would use camel-sap-netweaver component
                })
                .to("sap-netweaver://${docuflow.integrations.erp.sap.host}?client=${docuflow.integrations.erp.sap.client}&user=${docuflow.integrations.erp.sap.user}&password=${docuflow.integrations.erp.sap.password}&systemNumber=${docuflow.integrations.erp.sap.system-number}&language=${docuflow.integrations.erp.sap.language}")
                .choice()
                    .when(simple("${header.CamelSapStatus} == 'SUCCESS'"))
                        .log("SAP export successful")
                    .otherwise()
                        .log("SAP export failed: ${header.CamelSapErrorMessage}")
                        .to("rabbitmq:docuflow.exports.retry")
                .endChoice();
        }

        // NetSuite Export Route
        if (netsuiteEnabled) {
            from("direct:export-netsuite")
                .routeId("export-netsuite")
                .process(exchange -> {
                    // Transform to NetSuite SuiteTalk/REST format
                })
                .to("netsuite://${docuflow.integrations.erp.netsuite.account-id}?consumerKey=${docuflow.integrations.erp.netsuite.consumer-key}&consumerSecret=${docuflow.integrations.erp.netsuite.consumer-secret}&tokenId=${docuflow.integrations.erp.netsuite.token-id}&tokenSecret=${docuflow.integrations.erp.netsuite.token-secret}")
                .choice()
                    .when(simple("${header.CamelNetSuiteStatus} == 'SUCCESS'"))
                        .log("NetSuite export successful")
                    .otherwise()
                        .log("NetSuite export failed")
                        .to("rabbitmq:docuflow.exports.retry")
                .endChoice();
        }

        // Odoo Export Route
        if (odooEnabled) {
            from("direct:export-odoo")
                .routeId("export-odoo")
                .process(exchange -> {
                    // Transform to Odoo ORM format
                })
                .to("odoo://${docuflow.integrations.erp.odoo.url}?database=${docuflow.integrations.erp.odoo.database}&username=${docuflow.integrations.erp.odoo.username}&password=${docuflow.integrations.erp.odoo.password}")
                .choice()
                    .when(simple("${header.CamelOdooStatus} == 'SUCCESS'"))
                        .log("Odoo export successful")
                    .otherwise()
                        .log("Odoo export failed")
                        .to("rabbitmq:docuflow.exports.retry")
                .endChoice();
        }

        // Retry route for ERP exports
        from("rabbitmq:docuflow.exports.retry?queue=docuflow.exports.retry&autoDelete=false&durable=true")
            .routeId("erp-export-retry")
            .delay(simple("${header.retryDelay}"))
            .to("kafka:docuflow.exports.erp");
    }
}