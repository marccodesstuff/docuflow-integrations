package com.docuflow.integrations.health;

import org.apache.camel.health.HealthCheck;
import org.apache.camel.health.HealthCheckRegistry;
import org.apache.camel.health.HealthCheckResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class IntegrationHealthIndicator implements HealthIndicator {

    private final HealthCheckRegistry healthCheckRegistry;

    @Value("${docuflow.integrations.erp.sap.enabled:false}")
    private boolean sapEnabled;

    @Value("${docuflow.integrations.erp.netsuite.enabled:false}")
    private boolean netsuiteEnabled;

    @Value("${docuflow.integrations.erp.odoo.enabled:false}")
    private boolean odooEnabled;

    @Value("${docuflow.integrations.edi.enabled:false}")
    private boolean ediEnabled;

    @Value("${docuflow.integrations.email.enabled:false}")
    private boolean emailEnabled;

    @Value("${docuflow.integrations.sms.enabled:false}")
    private boolean smsEnabled;

    @Value("${docuflow.integrations.storage-sync.enabled:false}")
    private boolean storageSyncEnabled;

    public IntegrationHealthIndicator(HealthCheckRegistry healthCheckRegistry) {
        this.healthCheckRegistry = healthCheckRegistry;
    }

    @Override
    public Health health() {
        Health.Builder builder = Health.up();
        
        Map<String, HealthCheckResult> results = healthCheckRegistry.getHealthChecks();
        
        // Check Camel context health
        HealthCheckResult camelHealth = results.get("camel");
        if (camelHealth != null) {
            builder.withDetail("camel", camelHealth.getState().name());
        }
        
        // Check route statuses
        results.entrySet().stream()
            .filter(e -> e.getKey().startsWith("route."))
            .forEach(e -> builder.withDetail(e.getKey(), e.getValue().getState().name()));
        
        // Add integration config status
        builder.withDetail("sapEnabled", sapEnabled);
        builder.withDetail("netsuiteEnabled", netsuiteEnabled);
        builder.withDetail("odooEnabled", odooEnabled);
        builder.withDetail("ediEnabled", ediEnabled);
        builder.withDetail("emailEnabled", emailEnabled);
        builder.withDetail("smsEnabled", smsEnabled);
        builder.withDetail("storageSyncEnabled", storageSyncEnabled);
        
        // Overall health - check if any critical routes are down
        boolean criticalDown = results.values().stream()
            .filter(r -> r.getState() == HealthCheckResult.State.DOWN)
            .anyMatch(r -> r.getName().contains("webhook") || r.getName().contains("edi"));
        
        if (criticalDown) {
            return builder.down().build();
        }
        
        return builder.build();
    }
}