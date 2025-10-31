package com.minicloud.controlplane.service;

import com.minicloud.controlplane.config.IcebergConfiguration;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;

import javax.annotation.PostConstruct;

/**
 * Legacy Iceberg Catalog Service - now delegates to GlobalCatalogService.
 * This service is maintained for backward compatibility.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class IcebergCatalogService {
    
    private static final Logger logger = LoggerFactory.getLogger(IcebergCatalogService.class);
    
    private final IcebergConfiguration icebergConfig;
    private final GlobalCatalogService globalCatalogService;
    
    @Autowired
    public IcebergCatalogService(IcebergConfiguration icebergConfig, GlobalCatalogService globalCatalogService) {
        this.icebergConfig = icebergConfig;
        this.globalCatalogService = globalCatalogService;
    }
    
    @PostConstruct
    public void initialize() {
        if (!icebergConfig.isEnabled()) {
            logger.info("Iceberg integration is disabled");
            return;
        }
        
        logger.info("IcebergCatalogService initialized - delegating to GlobalCatalogService");
    }
    
    /**
     * Gets the Iceberg catalog instance from GlobalCatalogService.
     */
    public Catalog getCatalog() {
        return globalCatalogService.getCatalog();
    }
    
    /**
     * Gets the S3 client from GlobalCatalogService.
     */
    public S3Client getS3Client() {
        return globalCatalogService.getS3Client();
    }
    
    /**
     * Checks if Iceberg integration is enabled.
     */
    public boolean isEnabled() {
        return globalCatalogService.isEnabled();
    }
}