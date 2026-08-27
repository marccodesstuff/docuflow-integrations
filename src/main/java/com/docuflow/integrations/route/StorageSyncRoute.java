package com.docuflow.integrations.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class StorageSyncRoute extends RouteBuilder {

    @Value("${docuflow.integrations.storage-sync.enabled:false}")
    private boolean enabled;

    @Value("${docuflow.integrations.storage-sync.aws.enabled:false}")
    private boolean awsEnabled;

    @Value("${docuflow.integrations.storage-sync.aws.region:us-east-1}")
    private String awsRegion;

    @Value("${docuflow.integrations.storage-sync.aws.bucket:}")
    private String awsBucket;

    @Value("${docuflow.integrations.storage-sync.azure.enabled:false}")
    private boolean azureEnabled;

    @Value("${docuflow.integrations.storage-sync.azure.account-name:}")
    private String azureAccount;

    @Value("${docuflow.integrations.storage-sync.azure.container:}")
    private String azureContainer;

    @Value("${docuflow.integrations.storage-sync.gcp.enabled:false}")
    private boolean gcpEnabled;

    @Value("${docuflow.integrations.storage-sync.gcp.project-id:}")
    private String gcpProject;

    @Value("${docuflow.integrations.storage-sync.gcp.bucket:}")
    private String gcpBucket;

    @Override
    public void configure() {
        if (!enabled) {
            return;
        }

        // Sync processed documents to cloud storage
        from("kafka:docuflow.storage.sync?groupId=docuflow-integrations&autoOffsetReset=earliest")
            .routeId("storage-sync")
            .process(exchange -> {
                String targetCloud = exchange.getIn().getHeader("targetCloud", String.class);
                exchange.getIn().setHeader("targetCloud", targetCloud);
            })
            .choice()
                .when(simple("${header.targetCloud} == 'aws'"))
                    .to("direct:sync-aws")
                .when(simple("${header.targetCloud} == 'azure'"))
                    .to("direct:sync-azure")
                .when(simple("${header.targetCloud} == 'gcp'"))
                    .to("direct:sync-gcp")
                .otherwise()
                    .log("Unknown cloud provider: ${header.targetCloud}")
            .endChoice();

        // AWS S3 Sync
        if (awsEnabled) {
            from("direct:sync-aws")
                .routeId("sync-aws")
                .process(exchange -> {
                    String sourcePath = exchange.getIn().getHeader("sourcePath", String.class);
                    String destKey = exchange.getIn().getHeader("destKey", String.class);
                    
                    // Download from MinIO/S3 source and upload to AWS S3
                    exchange.getIn().setHeader("CamelAwsS3Key", destKey);
                    exchange.getIn().setHeader("CamelAwsS3Bucket", awsBucket);
                })
                .to("aws2-s3://${awsBucket}?region=${awsRegion}&autoCreateBucket=true")
                .choice()
                    .when(simple("${header.CamelAwsS3HttpStatus} == 200"))
                        .log("Synced to AWS S3: ${header.destKey}")
                    .otherwise()
                        .log("AWS S3 sync failed: ${header.CamelAwsS3HttpStatus}")
                        .to("rabbitmq:docuflow.storage.sync.retry")
                .endChoice();
        }

        // Azure Blob Storage Sync
        if (azureEnabled) {
            from("direct:sync-azure")
                .routeId("sync-azure")
                .process(exchange -> {
                    String sourcePath = exchange.getIn().getHeader("sourcePath", String.class);
                    String destBlob = exchange.getIn().getHeader("destBlob", String.class);
                    
                    exchange.getIn().setHeader("CamelAzureBlobName", destBlob);
                    exchange.getIn().setHeader("CamelAzureContainerName", azureContainer);
                })
                .to("azure-storage-blob://${azureAccount}/${azureContainer}?credentialType=ACCOUNT_KEY")
                .choice()
                    .when(simple("${header.CamelAzureBlobStatus} == 'SUCCESS'"))
                        .log("Synced to Azure Blob: ${header.destBlob}")
                    .otherwise()
                        .log("Azure Blob sync failed")
                        .to("rabbitmq:docuflow.storage.sync.retry")
                .endChoice();
        }

        // GCP Cloud Storage Sync
        if (gcpEnabled) {
            from("direct:sync-gcp")
                .routeId("sync-gcp")
                .process(exchange -> {
                    String sourcePath = exchange.getIn().getHeader("sourcePath", String.class);
                    String destObject = exchange.getIn().getHeader("destObject", String.class);
                    
                    exchange.getIn().setHeader("CamelGCPStorageObjectName", destObject);
                    exchange.getIn().setHeader("CamelGCPStorageBucketName", gcpBucket);
                })
                .to("google-storage://${gcpBucket}?projectId=${gcpProject}")
                .choice()
                    .when(simple("${header.CamelGCPStorageStatus} == 'SUCCESS'"))
                        .log("Synced to GCP: ${header.destObject}")
                    .otherwise()
                        .log("GCP sync failed")
                        .to("rabbitmq:docuflow.storage.sync.retry")
                .endChoice();
        }

        // Retry route
        from("rabbitmq:docuflow.storage.sync.retry?queue=docuflow.storage.sync.retry&autoDelete=false&durable=true")
            .routeId("storage-sync-retry")
            .delay(300000) // 5 minutes
            .to("kafka:docuflow.storage.sync");
    }
}