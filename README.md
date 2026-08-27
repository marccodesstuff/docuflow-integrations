# DocuFlow Integrations

Spring Boot + Apache Camel integration layer for ERP connectors, EDI processing, webhook delivery, notifications, and cloud storage synchronization.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   DocuFlow Integrations                     │
│  ┌────────────┐ ┌────────┐ ┌────────┐ ┌─────────────────┐  │
│  │    ERP     │ │  EDI   │ │Webhooks│ │ Notifications   │  │
│  │ Connectors │ │Processor│ │Delivery│ │ (Email/SMS)     │  │
│  └─────┬──────┘ └────┬───┘ └────┬──┘ └────────┬────────┘  │
│        │             │           │             │           │
│        ▼             ▼           ▼             ▼           │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                    Apache Camel Routes                  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
   ┌──────────┐         ┌──────────┐         ┌──────────┐
   │  Kafka   │         │ RabbitMQ │         │  Cloud   │
   │          │         │          │         │ Storage  │
   └──────────┘         └──────────┘         └──────────┘
```

## Integration Capabilities

### ERP Connectors

| System | Protocol | Operations |
|--------|----------|------------|
| **SAP** | RFC/BAPI (camel-sap-netweaver) | Invoice posting, PO creation, master data sync |
| **NetSuite** | SuiteTalk REST (camel-salesforce) | Record CRUD, saved searches, file cabinet |
| **Odoo** | XML-RPC/JSON-RPC | Model CRUD, workflow triggers, reports |

### EDI Processing

| Transaction | Description | Direction |
|-------------|-------------|-----------|
| **810** | Invoice | Inbound/Outbound |
| **850** | Purchase Order | Inbound/Outbound |
| **856** | Advance Ship Notice | Inbound/Outbound |
| **997** | Functional Acknowledgment | Outbound |

Protocols: AS2 (HTTP), FTP/SFTP, Local file, VAN

### Webhook Delivery

- **Reliable delivery** with exponential backoff (1m, 5m, 15m, 1h, 6h)
- **Dead letter queue** for failed deliveries
- **HMAC signature** verification
- **Idempotency keys** for duplicate protection
- **Configurable retry limits** per webhook

### Notifications

| Channel | Provider | Features |
|---------|----------|----------|
| **Email** | SMTP | Templating, attachments, tracking |
| **SMS** | Twilio / Vonage | Global delivery, opt-out handling |

### Cloud Storage Sync

- **AWS S3** - Multi-region, lifecycle policies
- **Azure Blob** - Hot/Cool/Archive tiers
- **GCP Cloud Storage** - Multi-regional, versioning

## Tech Stack

- **Java 21**, Spring Boot 3.3
- **Apache Camel 4.8** - 80+ components
- **Kafka** - Event streaming
- **RabbitMQ** - Message broker
- **Prometheus/Grafana** - Observability

## Quick Start

### Prerequisites

- JDK 21+
- Docker Compose

### Local Development

```bash
# Start dependencies
cd ../docuflow-infra/docker
docker compose up -d kafka rabbitmq postgres

# Build and run
./gradlew bootRun --args='--spring.profiles.active=dev'
```

### Enable Connectors

Configure via environment variables or `application.yml`:

```yaml
docuflow:
  integrations:
    erp:
      sap:
        enabled: true
        host: sap.example.com
        client: "100"
        user: "${SAP_USER}"
        password: "${SAP_PASSWORD}"
      netsuite:
        enabled: true
        account-id: "${NETSUITE_ACCOUNT}"
    edi:
      enabled: true
      partner-id: "PARTNER123"
    webhook:
      max-retries: 5
```

## Camel Routes

Defined in `src/main/java/com/docuflow/integrations/route/`:

- `WebhookDeliveryRoute` - Reliable webhook delivery
- `ErpExportRoute` - ERP system exports
- `EdiRoute` - EDI inbound/outbound processing
- `NotificationRoute` - Email/SMS notifications
- `StorageSyncRoute` - Cloud storage synchronization

## Monitoring

- **Health**: `GET /actuator/health` (includes route status)
- **Metrics**: `GET /actuator/prometheus`
- **Camel Routes**: `GET /actuator/camelroutes`
- **Grafana Dashboard**: DocuFlow Integrations Overview

## License

MIT