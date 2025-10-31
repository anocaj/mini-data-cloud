package com.minicloud.controlplane.service;

import com.minicloud.controlplane.config.IcebergConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.stream.Collectors;

@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class DatabaseInitializationService {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseInitializationService.class);
    
    private final IcebergConfiguration icebergConfig;
    
    @Autowired
    public DatabaseInitializationService(IcebergConfiguration icebergConfig) {
        this.icebergConfig = icebergConfig;
    }
    
    @PostConstruct
    public void initializeDatabase() {
        if (!icebergConfig.isEnabled()) {
            logger.info("Iceberg is disabled, skipping database initialization");
            return;
        }
        
        logger.info("Initializing PostgreSQL database for Iceberg catalog");
        
        try {
            // Wait for PostgreSQL to be ready
            waitForDatabase();
            
            // Execute schema initialization script
            executeSchemaScript();
            
            logger.info("Database initialization completed successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize database", e);
            // Don't throw exception here as the application should still start
            // The catalog service will handle the error when it tries to connect
        }
    }
    
    private void waitForDatabase() throws InterruptedException {
        int maxRetries = 30;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                logger.info("Attempting to connect to database (attempt {}/{})", retryCount + 1, maxRetries);
                
                try (Connection connection = DriverManager.getConnection(
                    icebergConfig.getCatalog().getUri(),
                    icebergConfig.getCatalog().getUsername(),
                    icebergConfig.getCatalog().getPassword())) {
                    
                    logger.info("Successfully connected to database");
                    return;
                }
            } catch (Exception e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    throw new RuntimeException("Could not connect to database after " + maxRetries + " attempts", e);
                }
                
                logger.warn("Database connection attempt {} failed, retrying in 2 seconds...", retryCount);
                Thread.sleep(2000);
            }
        }
    }
    
    private void executeSchemaScript() {
        try {
            logger.info("Executing Iceberg catalog schema script");
            
            // Read the schema script
            ClassPathResource resource = new ClassPathResource("db/iceberg-catalog-schema.sql");
            String schemaScript;
            
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
                schemaScript = reader.lines().collect(Collectors.joining("\n"));
            }
            
            // Execute the script
            try (Connection connection = DriverManager.getConnection(
                icebergConfig.getCatalog().getUri(),
                icebergConfig.getCatalog().getUsername(),
                icebergConfig.getCatalog().getPassword())) {
                
                // Split script into individual statements and execute them
                String[] statements = schemaScript.split(";");
                
                try (Statement statement = connection.createStatement()) {
                    for (String sql : statements) {
                        String trimmedSql = sql.trim();
                        if (!trimmedSql.isEmpty() && !trimmedSql.startsWith("--")) {
                            logger.debug("Executing SQL: {}", trimmedSql);
                            statement.execute(trimmedSql);
                        }
                    }
                }
                
                logger.info("Schema script executed successfully");
            }
            
        } catch (Exception e) {
            logger.error("Failed to execute schema script", e);
            throw new RuntimeException("Schema initialization failed", e);
        }
    }
}