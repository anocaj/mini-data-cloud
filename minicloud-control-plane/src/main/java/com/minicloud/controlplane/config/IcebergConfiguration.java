package com.minicloud.controlplane.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "minicloud.iceberg")
public class IcebergConfiguration {
    
    private boolean enabled = false;
    private CatalogConfig catalog = new CatalogConfig();
    private S3Config s3 = new S3Config();
    private CacheConfig cache = new CacheConfig();
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public CatalogConfig getCatalog() {
        return catalog;
    }
    
    public void setCatalog(CatalogConfig catalog) {
        this.catalog = catalog;
    }
    
    public S3Config getS3() {
        return s3;
    }
    
    public void setS3(S3Config s3) {
        this.s3 = s3;
    }
    
    public CacheConfig getCache() {
        return cache;
    }
    
    public void setCache(CacheConfig cache) {
        this.cache = cache;
    }
    
    public static class CatalogConfig {
        private String type = "jdbc";
        private String uri;
        private String username;
        private String password;
        private String warehouse;
        
        public String getType() {
            return type;
        }
        
        public void setType(String type) {
            this.type = type;
        }
        
        public String getUri() {
            return uri;
        }
        
        public void setUri(String uri) {
            this.uri = uri;
        }
        
        public String getUsername() {
            return username;
        }
        
        public void setUsername(String username) {
            this.username = username;
        }
        
        public String getPassword() {
            return password;
        }
        
        public void setPassword(String password) {
            this.password = password;
        }
        
        public String getWarehouse() {
            return warehouse;
        }
        
        public void setWarehouse(String warehouse) {
            this.warehouse = warehouse;
        }
    }
    
    public static class S3Config {
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private boolean pathStyleAccess = true;
        private String region = "us-east-1";
        
        public String getEndpoint() {
            return endpoint;
        }
        
        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }
        
        public String getAccessKey() {
            return accessKey;
        }
        
        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }
        
        public String getSecretKey() {
            return secretKey;
        }
        
        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }
        
        public boolean isPathStyleAccess() {
            return pathStyleAccess;
        }
        
        public void setPathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
        }
        
        public String getRegion() {
            return region;
        }
        
        public void setRegion(String region) {
            this.region = region;
        }
    }
    
    public static class CacheConfig {
        private int l1Size = 10000;
        private String l1Ttl = "300s";
        private int l2Size = 100000;
        private String l2Ttl = "3600s";
        private boolean l3Enabled = true;
        
        public int getL1Size() {
            return l1Size;
        }
        
        public void setL1Size(int l1Size) {
            this.l1Size = l1Size;
        }
        
        public String getL1Ttl() {
            return l1Ttl;
        }
        
        public void setL1Ttl(String l1Ttl) {
            this.l1Ttl = l1Ttl;
        }
        
        public int getL2Size() {
            return l2Size;
        }
        
        public void setL2Size(int l2Size) {
            this.l2Size = l2Size;
        }
        
        public String getL2Ttl() {
            return l2Ttl;
        }
        
        public void setL2Ttl(String l2Ttl) {
            this.l2Ttl = l2Ttl;
        }
        
        public boolean isL3Enabled() {
            return l3Enabled;
        }
        
        public void setL3Enabled(boolean l3Enabled) {
            this.l3Enabled = l3Enabled;
        }
    }
}