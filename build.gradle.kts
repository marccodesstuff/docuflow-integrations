plugins {
    id("java")
    id("org.springframework.boot") version "3.3.2"
    id("io.spring.dependency-management") version "1.1.6"
    id("com.google.protobuf") version "0.9.4"
    id("maven-publish")
}

group = "com.docuflow.integrations"
version = "1.0.0-SNAPSHOT"

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
}

repositories {
    mavenCentral()
    maven { url = uri("https://plugins.gradle.org/m2/") }
    maven { url = uri("https://maven.pkg.github.com/docuflow/docuflow-shared") }
}

dependencies {
    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    
    // Apache Camel
    implementation("org.apache.camel.springboot:camel-spring-boot-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-http-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-kafka-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-rabbitmq-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-jms-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-file-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-ftp-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-sftp-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-salesforce-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-netty-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-mina-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-sap-netweaver-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-sap-odata-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-jackson-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-gson-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-xml-jaxb-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-csv-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-bindy-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-flatpack-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-edi-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-smpp-starter:4.8.0")
    implementation("org.apache.camel.springboot:camel-sjms2-starter:4.8.0")
    
    // DocuFlow shared contracts
    implementation("com.docuflow.shared:docuflow-shared:1.0.0-SNAPSHOT")
    
    // Messaging
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework.amqp:spring-rabbit")
    
    // HTTP Client
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    
    // Utilities
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.apache.commons:commons-lang3:3.15.0")
    implementation("com.google.guava:guava:33.2.1-jre")
    
    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.apache.camel:camel-test-spring-junit5:4.8.0")
    testImplementation("org.testcontainers:junit-jupiter:1.20.1")
    testImplementation("org.testcontainers:kafka:1.20.1")
    testImplementation("org.testcontainers:rabbitmq:1.20.1")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.cloud:spring-cloud-dependencies:2023.0.3")
        mavenBom("org.apache.camel:camel-bom:4.8.0")
        mavenBom("org.testcontainers:testcontainers-bom:1.20.1")
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.65.1"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
            task.builtins {
                id("java")
            }
        }
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs << "-Xlint:unchecked" << "-Xlint:deprecation" << "-parameters"
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
    systemProperty("spring.profiles.active", "test")
}

springBoot {
    buildInfo()
}