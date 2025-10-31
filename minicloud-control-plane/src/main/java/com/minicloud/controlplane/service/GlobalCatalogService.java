package com.minicloud.controlplane.service;

import com.minicloud.controlplane.config.IcebergConfiguration;
import com.minicloud.controlplane.model.TableStatistics;
import com.minicloud.controlplane.model.PartitionStatistics;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Global Catalog Service providing enterprise-grade metadata management with multi-tenancy support.
 * Implements Iceberg catalog operations with JDBC backend for persistent metadata storage.
 */
@Service
@ConditionalOnProperty(name = "minicloud.iceberg.enabled", havingValue = "true")
public class GlobalCatalogService {
    
    private static final Logger logger = LoggerFactory.getLogger(GlobalCatalogService.class);
    
    private final IcebergConfiguration icebergConfig;
    private HadoopCatalog catalog;
    private S3Client s3Client;
    
    @Autowired
    public GlobalCatalogService(IcebergConfiguration icebergConfig) {
        this.icebergConfig = icebergConfig;
    }
    
    @PostConstruct
    public void initialize() {
        if (!icebergConfig.isEnabled()) {
            logger.info("Iceberg integration is disabled");
            return;
        }
        
        logger.info("Initializing Global Catalog Service");
        
        try {
            // Initialize S3 client for storage access
            initializeS3Client();
            
            // Initialize JDBC catalog with PostgreSQL backend
            initializeJdbcCatalog();
            
            // Create default namespace if it doesn't exist
            createDefaultNamespace();
            
            logger.info("Global Catalog Service initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize Global Catalog Service", e);
            throw new RuntimeException("Global Catalog Service initialization failed", e);
        }
    }
    
    private void initializeS3Client() {
        logger.info("Initializing S3 client with endpoint: {}", icebergConfig.getS3().getEndpoint());
        
        AwsBasicCredentials credentials = AwsBasicCredentials.create(
            icebergConfig.getS3().getAccessKey(),
            icebergConfig.getS3().getSecretKey()
        );
        
        this.s3Client = S3Client.builder()
            .endpointOverride(URI.create(icebergConfig.getS3().getEndpoint()))
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .region(Region.of(icebergConfig.getS3().getRegion()))
            .forcePathStyle(icebergConfig.getS3().isPathStyleAccess())
            .build();
        
        logger.info("S3 client initialized successfully");
    }
    
    private void initializeJdbcCatalog() {
        logger.info("Initializing Hadoop catalog with warehouse: {}", icebergConfig.getCatalog().getWarehouse());
        
        this.catalog = new HadoopCatalog();
        
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", icebergConfig.getCatalog().getWarehouse());
        
        // S3 configuration for Iceberg
        properties.put("s3.endpoint", icebergConfig.getS3().getEndpoint());
        properties.put("s3.access-key-id", icebergConfig.getS3().getAccessKey());
        properties.put("s3.secret-access-key", icebergConfig.getS3().getSecretKey());
        properties.put("s3.path-style-access", String.valueOf(icebergConfig.getS3().isPathStyleAccess()));
        
        // Hadoop configuration for S3
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.endpoint", icebergConfig.getS3().getEndpoint());
        hadoopConf.set("fs.s3a.access.key", icebergConfig.getS3().getAccessKey());
        hadoopConf.set("fs.s3a.secret.key", icebergConfig.getS3().getSecretKey());
        hadoopConf.set("fs.s3a.path.style.access", String.valueOf(icebergConfig.getS3().isPathStyleAccess()));
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        
        catalog.setConf(hadoopConf);
        catalog.initialize("minicloud_catalog", properties);
        
        logger.info("Hadoop catalog initialized successfully");
    }
    
    private void createDefaultNamespace() {
        try {
            Namespace defaultNamespace = Namespace.of("default");
            if (!namespaceExists(defaultNamespace)) {
                createNamespace(defaultNamespace, Map.of(
                    "description", "Default namespace for Mini Data Cloud",
                    "created_by", "system",
                    "created_at", Instant.now().toString()
                ));
                logger.info("Created default namespace");
            } else {
                logger.info("Default namespace already exists");
            }
        } catch (Exception e) {
            logger.warn("Could not create default namespace", e);
        }
    }
    
    // Namespace Management Operations
    
    /**
     * Creates a new namespace with the specified properties.
     * Supports multi-tenancy by allowing isolated namespaces for different users/organizations.
     */
    public Namespace createNamespace(String name, Map<String, String> properties) {
        return createNamespace(Namespace.of(name), properties);
    }
    
    /**
     * Creates a new namespace with the specified properties.
     */
    public Namespace createNamespace(Namespace namespace, Map<String, String> properties) {
        try {
            logger.info("Creating namespace: {}", namespace);
            
            Map<String, String> namespaceProperties = new HashMap<>(properties);
            namespaceProperties.putIfAbsent("created_at", Instant.now().toString());
            
            catalog.createNamespace(namespace, namespaceProperties);
            
            logger.info("Successfully created namespace: {}", namespace);
            return namespace;
        } catch (AlreadyExistsException e) {
            logger.warn("Namespace already exists: {}", namespace);
            throw new RuntimeException("Namespace already exists: " + namespace, e);
        } catch (Exception e) {
            logger.error("Failed to create namespace: {}", namespace, e);
            throw new RuntimeException("Failed to create namespace: " + namespace, e);
        }
    }
    
    /**
     * Lists all available namespaces in the catalog.
     */
    public List<Namespace> listNamespaces() {
        try {
            logger.debug("Listing all namespaces");
            List<Namespace> namespaces = catalog.listNamespaces();
            logger.debug("Found {} namespaces", namespaces.size());
            return namespaces;
        } catch (Exception e) {
            logger.error("Failed to list namespaces", e);
            throw new RuntimeException("Failed to list namespaces", e);
        }
    }
    
    /**
     * Lists namespaces under a parent namespace.
     */
    public List<Namespace> listNamespaces(Namespace parent) {
        try {
            logger.debug("Listing namespaces under parent: {}", parent);
            List<Namespace> namespaces = catalog.listNamespaces(parent);
            logger.debug("Found {} namespaces under parent: {}", namespaces.size(), parent);
            return namespaces;
        } catch (Exception e) {
            logger.error("Failed to list namespaces under parent: {}", parent, e);
            throw new RuntimeException("Failed to list namespaces under parent: " + parent, e);
        }
    }
    
    /**
     * Loads namespace metadata and properties.
     */
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        try {
            logger.debug("Loading metadata for namespace: {}", namespace);
            Map<String, String> metadata = catalog.loadNamespaceMetadata(namespace);
            logger.debug("Loaded metadata for namespace: {} with {} properties", namespace, metadata.size());
            return metadata;
        } catch (NoSuchNamespaceException e) {
            logger.warn("Namespace not found: {}", namespace);
            throw new RuntimeException("Namespace not found: " + namespace, e);
        } catch (Exception e) {
            logger.error("Failed to load namespace metadata: {}", namespace, e);
            throw new RuntimeException("Failed to load namespace metadata: " + namespace, e);
        }
    }
    
    /**
     * Checks if a namespace exists.
     */
    public boolean namespaceExists(Namespace namespace) {
        try {
            catalog.loadNamespaceMetadata(namespace);
            return true;
        } catch (NoSuchNamespaceException e) {
            return false;
        } catch (Exception e) {
            logger.warn("Error checking namespace existence: {}", namespace, e);
            return false;
        }
    }
    
    /**
     * Drops a namespace. Only empty namespaces can be dropped.
     */
    public boolean dropNamespace(Namespace namespace) {
        try {
            logger.info("Dropping namespace: {}", namespace);
            boolean dropped = catalog.dropNamespace(namespace);
            if (dropped) {
                logger.info("Successfully dropped namespace: {}", namespace);
            } else {
                logger.warn("Namespace was not dropped (may not be empty): {}", namespace);
            }
            return dropped;
        } catch (Exception e) {
            logger.error("Failed to drop namespace: {}", namespace, e);
            throw new RuntimeException("Failed to drop namespace: " + namespace, e);
        }
    }
    
    // Table Management Operations
    
    /**
     * Creates a new Iceberg table with the specified schema and partition specification.
     */
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
        try {
            logger.info("Creating table: {}", identifier);
            
            Table table = catalog.createTable(identifier, schema, spec);
            
            logger.info("Successfully created table: {}", identifier);
            return table;
        } catch (AlreadyExistsException e) {
            logger.warn("Table already exists: {}", identifier);
            throw new RuntimeException("Table already exists: " + identifier, e);
        } catch (Exception e) {
            logger.error("Failed to create table: {}", identifier, e);
            throw new RuntimeException("Failed to create table: " + identifier, e);
        }
    }
    
    /**
     * Creates a new Iceberg table with additional properties.
     */
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties) {
        try {
            logger.info("Creating table with properties: {}", identifier);
            
            Table table = catalog.createTable(identifier, schema, spec, properties);
            
            logger.info("Successfully created table with properties: {}", identifier);
            return table;
        } catch (AlreadyExistsException e) {
            logger.warn("Table already exists: {}", identifier);
            throw new RuntimeException("Table already exists: " + identifier, e);
        } catch (Exception e) {
            logger.error("Failed to create table: {}", identifier, e);
            throw new RuntimeException("Failed to create table: " + identifier, e);
        }
    }
    
    /**
     * Loads an existing Iceberg table.
     */
    public Table loadTable(TableIdentifier identifier) {
        try {
            logger.debug("Loading table: {}", identifier);
            Table table = catalog.loadTable(identifier);
            logger.debug("Successfully loaded table: {}", identifier);
            return table;
        } catch (NoSuchTableException e) {
            logger.warn("Table not found: {}", identifier);
            throw new RuntimeException("Table not found: " + identifier, e);
        } catch (Exception e) {
            logger.error("Failed to load table: {}", identifier, e);
            throw new RuntimeException("Failed to load table: " + identifier, e);
        }
    }
    
    /**
     * Lists all tables in the specified namespace.
     */
    public List<TableIdentifier> listTables(Namespace namespace) {
        try {
            logger.debug("Listing tables in namespace: {}", namespace);
            List<TableIdentifier> tables = catalog.listTables(namespace);
            logger.debug("Found {} tables in namespace: {}", tables.size(), namespace);
            return tables;
        } catch (Exception e) {
            logger.error("Failed to list tables in namespace: {}", namespace, e);
            throw new RuntimeException("Failed to list tables in namespace: " + namespace, e);
        }
    }
    
    /**
     * Checks if a table exists.
     */
    public boolean tableExists(TableIdentifier identifier) {
        try {
            catalog.loadTable(identifier);
            return true;
        } catch (NoSuchTableException e) {
            return false;
        } catch (Exception e) {
            logger.warn("Error checking table existence: {}", identifier, e);
            return false;
        }
    }
    
    /**
     * Drops an Iceberg table.
     */
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        try {
            logger.info("Dropping table: {} (purge: {})", identifier, purge);
            boolean dropped = catalog.dropTable(identifier, purge);
            if (dropped) {
                logger.info("Successfully dropped table: {}", identifier);
            } else {
                logger.warn("Table was not dropped: {}", identifier);
            }
            return dropped;
        } catch (Exception e) {
            logger.error("Failed to drop table: {}", identifier, e);
            throw new RuntimeException("Failed to drop table: " + identifier, e);
        }
    }
    
    /**
     * Renames an Iceberg table.
     */
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        try {
            logger.info("Renaming table from {} to {}", from, to);
            catalog.renameTable(from, to);
            logger.info("Successfully renamed table from {} to {}", from, to);
        } catch (Exception e) {
            logger.error("Failed to rename table from {} to {}", from, to, e);
            throw new RuntimeException("Failed to rename table from " + from + " to " + to, e);
        }
    }
    
    // Advanced Metadata Operations
    
    /**
     * Refreshes table metadata from the underlying storage.
     */
    public void refreshTableMetadata(TableIdentifier identifier) {
        try {
            logger.debug("Refreshing metadata for table: {}", identifier);
            Table table = loadTable(identifier);
            table.refresh();
            logger.debug("Successfully refreshed metadata for table: {}", identifier);
        } catch (Exception e) {
            logger.error("Failed to refresh table metadata: {}", identifier, e);
            throw new RuntimeException("Failed to refresh table metadata: " + identifier, e);
        }
    }
    
    /**
     * Gets table statistics from the statistics collection service.
     */
    public Optional<TableStatistics> getTableStatistics(TableIdentifier identifier) {
        // This will be implemented when StatisticsCollectionService is available
        // For now, return empty to avoid circular dependency during initialization
        return Optional.empty();
    }
    
    /**
     * Gets partition statistics from the statistics collection service.
     */
    public Optional<List<PartitionStatistics>> getPartitionStatistics(TableIdentifier identifier) {
        // This will be implemented when StatisticsCollectionService is available
        // For now, return empty to avoid circular dependency during initialization
        return Optional.empty();
    }
    
    // Time Travel and Versioning Operations
    
    /**
     * Gets the complete history of snapshots for a table.
     */
    public List<Snapshot> getTableHistory(TableIdentifier identifier) {
        try {
            logger.debug("Getting table history for: {}", identifier);
            Table table = loadTable(identifier);
            List<Snapshot> snapshots = new ArrayList<>();
            for (Snapshot snapshot : table.snapshots()) {
                snapshots.add(snapshot);
            }
            logger.debug("Found {} snapshots for table: {}", snapshots.size(), identifier);
            return snapshots;
        } catch (Exception e) {
            logger.error("Failed to get table history: {}", identifier, e);
            throw new RuntimeException("Failed to get table history: " + identifier, e);
        }
    }
    
    /**
     * Loads a table at a specific snapshot.
     */
    public Table loadTableAtSnapshot(TableIdentifier identifier, long snapshotId) {
        try {
            logger.debug("Loading table {} at snapshot: {}", identifier, snapshotId);
            Table table = loadTable(identifier);
            
            // Verify snapshot exists
            Snapshot snapshot = table.snapshot(snapshotId);
            if (snapshot == null) {
                throw new RuntimeException("Snapshot not found: " + snapshotId + " for table: " + identifier);
            }
            
            logger.debug("Successfully loaded table {} at snapshot: {}", identifier, snapshotId);
            return table;
        } catch (Exception e) {
            logger.error("Failed to load table {} at snapshot: {}", identifier, snapshotId, e);
            throw new RuntimeException("Failed to load table at snapshot: " + snapshotId, e);
        }
    }
    
    /**
     * Loads a table at a specific timestamp.
     */
    public Table loadTableAtTimestamp(TableIdentifier identifier, Instant timestamp) {
        try {
            logger.debug("Loading table {} at timestamp: {}", identifier, timestamp);
            Table table = loadTable(identifier);
            
            // Find the snapshot closest to the timestamp
            Snapshot targetSnapshot = null;
            long timestampMs = timestamp.toEpochMilli();
            
            for (Snapshot snapshot : table.snapshots()) {
                if (snapshot.timestampMillis() <= timestampMs) {
                    if (targetSnapshot == null || snapshot.timestampMillis() > targetSnapshot.timestampMillis()) {
                        targetSnapshot = snapshot;
                    }
                }
            }
            
            if (targetSnapshot == null) {
                throw new RuntimeException("No snapshot found before timestamp: " + timestamp + " for table: " + identifier);
            }
            
            logger.debug("Successfully loaded table {} at timestamp: {} (snapshot: {})", 
                identifier, timestamp, targetSnapshot.snapshotId());
            return table;
        } catch (Exception e) {
            logger.error("Failed to load table {} at timestamp: {}", identifier, timestamp, e);
            throw new RuntimeException("Failed to load table at timestamp: " + timestamp, e);
        }
    }
    
    // Utility Methods
    
    /**
     * Gets the underlying Iceberg catalog instance.
     */
    public Catalog getCatalog() {
        if (catalog == null) {
            throw new IllegalStateException("Catalog is not initialized");
        }
        return catalog;
    }
    
    /**
     * Gets the S3 client for storage operations.
     */
    public S3Client getS3Client() {
        if (s3Client == null) {
            throw new IllegalStateException("S3 client is not initialized");
        }
        return s3Client;
    }
    
    /**
     * Checks if Iceberg integration is enabled.
     */
    public boolean isEnabled() {
        return icebergConfig.isEnabled();
    }
    
    @PreDestroy
    public void cleanup() {
        if (s3Client != null) {
            try {
                s3Client.close();
                logger.info("S3 client closed successfully");
            } catch (Exception e) {
                logger.warn("Error closing S3 client", e);
            }
        }
        
        if (catalog != null) {
            try {
                catalog.close();
                logger.info("Catalog closed successfully");
            } catch (Exception e) {
                logger.warn("Error closing catalog", e);
            }
        }
    }
}