package com.minicloud.controlplane.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IcebergConfigurationTest {

    @Test
    void testIcebergConfigurationCreation() {
        // Test that the configuration class can be instantiated
        IcebergConfiguration config = new IcebergConfiguration();
        
        // Test default values
        assertFalse(config.isEnabled());
        assertNotNull(config.getCatalog());
        assertNotNull(config.getS3());
        assertNotNull(config.getCache());
        
        // Test catalog configuration
        IcebergConfiguration.CatalogConfig catalog = config.getCatalog();
        assertEquals("jdbc", catalog.getType());
        
        // Test S3 configuration
        IcebergConfiguration.S3Config s3 = config.getS3();
        assertTrue(s3.isPathStyleAccess());
        assertEquals("us-east-1", s3.getRegion());
        
        // Test cache configuration
        IcebergConfiguration.CacheConfig cache = config.getCache();
        assertEquals(10000, cache.getL1Size());
        assertEquals("300s", cache.getL1Ttl());
        assertTrue(cache.isL3Enabled());
    }
    
    @Test
    void testConfigurationSetters() {
        IcebergConfiguration config = new IcebergConfiguration();
        
        // Test enabling Iceberg
        config.setEnabled(true);
        assertTrue(config.isEnabled());
        
        // Test catalog configuration
        IcebergConfiguration.CatalogConfig catalog = new IcebergConfiguration.CatalogConfig();
        catalog.setWarehouse("s3a://test-bucket/");
        config.setCatalog(catalog);
        assertEquals("s3a://test-bucket/", config.getCatalog().getWarehouse());
        
        // Test S3 configuration
        IcebergConfiguration.S3Config s3 = new IcebergConfiguration.S3Config();
        s3.setEndpoint("http://localhost:9000");
        s3.setAccessKey("testkey");
        s3.setSecretKey("testsecret");
        config.setS3(s3);
        assertEquals("http://localhost:9000", config.getS3().getEndpoint());
        assertEquals("testkey", config.getS3().getAccessKey());
        assertEquals("testsecret", config.getS3().getSecretKey());
    }
}