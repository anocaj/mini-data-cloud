package com.minicloud.controlplane.service;

import com.minicloud.controlplane.config.IcebergConfiguration;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

/**
 * Unit tests for IcebergCatalogService focusing on backward compatibility and delegation.
 * Tests that the legacy service properly delegates to GlobalCatalogService.
 */
@ExtendWith(MockitoExtension.class)
class IcebergCatalogServiceTest {
    
    @Mock
    private IcebergConfiguration icebergConfig;
    
    @Mock
    private GlobalCatalogService globalCatalogService;
    
    @Mock
    private Catalog mockCatalog;
    
    @Mock
    private S3Client mockS3Client;
    
    private IcebergCatalogService icebergCatalogService;
    
    @BeforeEach
    void setUp() {
        // Configure mocks with lenient stubbing
        lenient().when(icebergConfig.isEnabled()).thenReturn(false); // Disable actual initialization
        
        icebergCatalogService = new IcebergCatalogService(icebergConfig, globalCatalogService);
    }
    
    // Initialization Tests
    
    @Test
    void testInitialization_WhenDisabled() {
        // Given & When & Then - Test basic initialization
        assertDoesNotThrow(() -> {
            IcebergCatalogService service = new IcebergCatalogService(icebergConfig, globalCatalogService);
            assertNotNull(service);
        });
    }
    
    @Test
    void testInitialization_WhenEnabled() {
        // Given & When & Then - Test initialization without error
        assertDoesNotThrow(() -> {
            IcebergCatalogService service = new IcebergCatalogService(icebergConfig, globalCatalogService);
            assertNotNull(service);
        });
    }
    
    // Delegation Tests
    
    @Test
    void testGetCatalog_DelegatesToGlobalService() {
        // Given
        when(globalCatalogService.getCatalog()).thenReturn(mockCatalog);
        
        // When
        Catalog result = icebergCatalogService.getCatalog();
        
        // Then
        assertEquals(mockCatalog, result);
        verify(globalCatalogService).getCatalog();
    }
    
    @Test
    void testGetS3Client_DelegatesToGlobalService() {
        // Given
        when(globalCatalogService.getS3Client()).thenReturn(mockS3Client);
        
        // When
        S3Client result = icebergCatalogService.getS3Client();
        
        // Then
        assertEquals(mockS3Client, result);
        verify(globalCatalogService).getS3Client();
    }
    
    @Test
    void testIsEnabled_DelegatesToGlobalService() {
        // Given
        when(globalCatalogService.isEnabled()).thenReturn(true);
        
        // When
        boolean result = icebergCatalogService.isEnabled();
        
        // Then
        assertTrue(result);
        verify(globalCatalogService).isEnabled();
    }
    
    @Test
    void testIsEnabled_WhenDisabled() {
        // Given
        when(globalCatalogService.isEnabled()).thenReturn(false);
        
        // When
        boolean result = icebergCatalogService.isEnabled();
        
        // Then
        assertFalse(result);
        verify(globalCatalogService).isEnabled();
    }
    
    // Error Handling Tests
    
    @Test
    void testGetCatalog_WhenGlobalServiceThrows() {
        // Given
        when(globalCatalogService.getCatalog()).thenThrow(new IllegalStateException("Catalog not initialized"));
        
        // When & Then
        assertThrows(IllegalStateException.class, () -> icebergCatalogService.getCatalog());
        verify(globalCatalogService).getCatalog();
    }
    
    @Test
    void testGetS3Client_WhenGlobalServiceThrows() {
        // Given
        when(globalCatalogService.getS3Client()).thenThrow(new IllegalStateException("S3 client not initialized"));
        
        // When & Then
        assertThrows(IllegalStateException.class, () -> icebergCatalogService.getS3Client());
        verify(globalCatalogService).getS3Client();
    }
    
    // Backward Compatibility Tests
    
    @Test
    void testBackwardCompatibility_AllMethodsExist() {
        // When & Then - Test that all expected methods exist
        assertDoesNotThrow(() -> {
            icebergCatalogService.getCatalog();
        });
        
        assertDoesNotThrow(() -> {
            icebergCatalogService.getS3Client();
        });
        
        assertDoesNotThrow(() -> {
            icebergCatalogService.isEnabled();
        });
    }
    
    @Test
    void testBackwardCompatibility_ServiceInterface() {
        // Given & When & Then - Test service can be used as expected
        assertNotNull(icebergCatalogService);
        
        // Verify it has the expected dependencies
        assertDoesNotThrow(() -> {
            // These calls should delegate to GlobalCatalogService
            when(globalCatalogService.isEnabled()).thenReturn(false);
            boolean enabled = icebergCatalogService.isEnabled();
            assertFalse(enabled);
        });
    }
    
    // Configuration Tests
    
    @Test
    void testConfiguration_InjectedCorrectly() {
        // Given & When & Then
        assertNotNull(icebergConfig);
        assertNotNull(globalCatalogService);
        
        // Verify the service was constructed with the right dependencies
        assertDoesNotThrow(() -> {
            IcebergCatalogService service = new IcebergCatalogService(icebergConfig, globalCatalogService);
            assertNotNull(service);
        });
    }
    
    @Test
    void testConfiguration_EnabledState() {
        // Given
        when(icebergConfig.isEnabled()).thenReturn(true);
        when(globalCatalogService.isEnabled()).thenReturn(true);
        
        // When
        boolean configEnabled = icebergConfig.isEnabled();
        boolean serviceEnabled = icebergCatalogService.isEnabled();
        
        // Then
        assertTrue(configEnabled);
        assertTrue(serviceEnabled);
    }
    
    // Integration Tests
    
    @Test
    void testIntegration_WithGlobalCatalogService() {
        // Given
        when(globalCatalogService.isEnabled()).thenReturn(true);
        when(globalCatalogService.getCatalog()).thenReturn(mockCatalog);
        when(globalCatalogService.getS3Client()).thenReturn(mockS3Client);
        
        // When
        boolean enabled = icebergCatalogService.isEnabled();
        Catalog catalog = icebergCatalogService.getCatalog();
        S3Client s3Client = icebergCatalogService.getS3Client();
        
        // Then
        assertTrue(enabled);
        assertEquals(mockCatalog, catalog);
        assertEquals(mockS3Client, s3Client);
        
        // Verify all calls were delegated
        verify(globalCatalogService).isEnabled();
        verify(globalCatalogService).getCatalog();
        verify(globalCatalogService).getS3Client();
    }
    
    @Test
    void testIntegration_WhenGlobalServiceDisabled() {
        // Given
        when(globalCatalogService.isEnabled()).thenReturn(false);
        when(globalCatalogService.getCatalog()).thenThrow(new IllegalStateException("Not initialized"));
        when(globalCatalogService.getS3Client()).thenThrow(new IllegalStateException("Not initialized"));
        
        // When
        boolean enabled = icebergCatalogService.isEnabled();
        
        // Then
        assertFalse(enabled);
        
        // Should throw when trying to get catalog or S3 client
        assertThrows(IllegalStateException.class, () -> icebergCatalogService.getCatalog());
        assertThrows(IllegalStateException.class, () -> icebergCatalogService.getS3Client());
    }
    
    // Service Lifecycle Tests
    
    @Test
    void testServiceLifecycle_Construction() {
        // Given & When
        IcebergCatalogService service = new IcebergCatalogService(icebergConfig, globalCatalogService);
        
        // Then
        assertNotNull(service);
    }
    
    @Test
    void testServiceLifecycle_MultipleInstances() {
        // Given & When
        IcebergCatalogService service1 = new IcebergCatalogService(icebergConfig, globalCatalogService);
        IcebergCatalogService service2 = new IcebergCatalogService(icebergConfig, globalCatalogService);
        
        // Then
        assertNotNull(service1);
        assertNotNull(service2);
        assertNotSame(service1, service2);
    }
    
    // Basic Construction Tests
    
    @Test
    void testBasicConstruction() {
        // When & Then - Test basic service construction
        assertDoesNotThrow(() -> {
            IcebergCatalogService service = new IcebergCatalogService(icebergConfig, globalCatalogService);
            assertNotNull(service);
        });
    }
}