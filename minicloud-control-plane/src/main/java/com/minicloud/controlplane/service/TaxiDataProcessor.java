package com.minicloud.controlplane.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;

/**
 * Processes NYC taxi trip data with intelligent partitioning strategies.
 * Handles both Yellow and Green taxi datasets with date-based and location-based partitioning.
 */
@Service
public class TaxiDataProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(TaxiDataProcessor.class);
    
    @Autowired
    private DataLoadingService dataLoadingService;
    
    @Autowired
    private PartitioningStrategy partitioningStrategy;
    
    @Value("${minicloud.nyc-data.datasets.taxi-yellow.years:[2024]}")
    private List<Integer> yellowTaxiYears;
    
    @Value("${minicloud.nyc-data.datasets.taxi-green.years:[2024]}")
    private List<Integer> greenTaxiYears;
    
    @Value("${minicloud.storage.data-directory:./data}")
    private String dataDirectory;
    
    private volatile String loadingStatus = "NOT_STARTED";
    
    /**
     * Load sample taxi data for demonstration
     */
    public void loadSampleTaxiData() {
        logger.info("Starting to load sample NYC taxi data...");
        loadingStatus = "LOADING";
        
        try {
            // Generate and load Yellow taxi sample data
            generateAndLoadYellowTaxiData();
            
            // Generate and load Green taxi sample data
            generateAndLoadGreenTaxiData();
            
            loadingStatus = "COMPLETED";
            logger.info("Successfully loaded NYC taxi sample data");
            
        } catch (Exception e) {
            loadingStatus = "FAILED: " + e.getMessage();
            logger.error("Failed to load NYC taxi data", e);
            throw new RuntimeException("Failed to load taxi data", e);
        }
    }
    
    /**
     * Generate and load Yellow taxi sample data
     */
    private void generateAndLoadYellowTaxiData() throws IOException {
        logger.info("Generating Yellow taxi sample data...");
        
        String csvFilePath = generateYellowTaxiCsv();
        
        // Load with intelligent partitioning
        DataLoadingService.LoadResult result = dataLoadingService.loadCsvData(
            csvFilePath, "nyc", "taxi_yellow", true);
        
        logger.info("Loaded Yellow taxi data: {} rows", result.getRowCount());
        
        // Apply intelligent partitioning strategy
        partitioningStrategy.applyTaxiTripPartitioning("nyc", "taxi_yellow");
    }
    
    /**
     * Generate and load Green taxi sample data
     */
    private void generateAndLoadGreenTaxiData() throws IOException {
        logger.info("Generating Green taxi sample data...");
        
        String csvFilePath = generateGreenTaxiCsv();
        
        // Load with intelligent partitioning
        DataLoadingService.LoadResult result = dataLoadingService.loadCsvData(
            csvFilePath, "nyc", "taxi_green", true);
        
        logger.info("Loaded Green taxi data: {} rows", result.getRowCount());
        
        // Apply intelligent partitioning strategy
        partitioningStrategy.applyTaxiTripPartitioning("nyc", "taxi_green");
    }
    
    /**
     * Generate Yellow taxi CSV sample data
     */
    private String generateYellowTaxiCsv() throws IOException {
        String csvFilePath = dataDirectory + "/sample-nyc-yellow-taxi.csv";
        File csvFile = new File(csvFilePath);
        csvFile.getParentFile().mkdirs();
        
        Random random = new Random(42); // Fixed seed for reproducible data
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
            // Write header
            writer.write("vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code_id,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,total_amount,pickup_location_id,dropoff_location_id");
            writer.newLine();
            
            // Generate sample records (1000 records for demo)
            for (int i = 0; i < 1000; i++) {
                LocalDateTime pickupTime = LocalDateTime.of(2024, 
                    random.nextInt(12) + 1, // Month 1-12
                    random.nextInt(28) + 1, // Day 1-28
                    random.nextInt(24),     // Hour 0-23
                    random.nextInt(60));    // Minute 0-59
                
                LocalDateTime dropoffTime = pickupTime.plusMinutes(random.nextInt(120) + 5); // 5-125 minutes trip
                
                int passengerCount = random.nextInt(6) + 1; // 1-6 passengers
                double tripDistance = 0.5 + random.nextDouble() * 20; // 0.5-20.5 miles
                
                // NYC coordinates (approximate)
                double pickupLon = -74.0 + random.nextDouble() * 0.3; // -74.0 to -73.7
                double pickupLat = 40.7 + random.nextDouble() * 0.3;  // 40.7 to 41.0
                double dropoffLon = -74.0 + random.nextDouble() * 0.3;
                double dropoffLat = 40.7 + random.nextDouble() * 0.3;
                
                int rateCodeId = random.nextInt(6) + 1; // 1-6
                String storeAndFwd = random.nextBoolean() ? "Y" : "N";
                int paymentType = random.nextInt(4) + 1; // 1-4
                
                double fareAmount = 2.5 + tripDistance * 2.5 + random.nextDouble() * 5;
                double extra = random.nextBoolean() ? 0.5 : 0.0;
                double mtaTax = 0.5;
                double tipAmount = random.nextDouble() * fareAmount * 0.2; // 0-20% tip
                double tollsAmount = random.nextBoolean() ? random.nextDouble() * 10 : 0.0;
                double totalAmount = fareAmount + extra + mtaTax + tipAmount + tollsAmount;
                
                int pickupLocationId = random.nextInt(265) + 1; // NYC taxi zones 1-265
                int dropoffLocationId = random.nextInt(265) + 1;
                
                writer.write(String.format("%d,%s,%s,%d,%.2f,%.6f,%.6f,%d,%s,%.6f,%.6f,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d",
                    random.nextInt(2) + 1, // vendor_id (1 or 2)
                    pickupTime.format(formatter),
                    dropoffTime.format(formatter),
                    passengerCount,
                    tripDistance,
                    pickupLon,
                    pickupLat,
                    rateCodeId,
                    storeAndFwd,
                    dropoffLon,
                    dropoffLat,
                    paymentType,
                    fareAmount,
                    extra,
                    mtaTax,
                    tipAmount,
                    tollsAmount,
                    totalAmount,
                    pickupLocationId,
                    dropoffLocationId));
                writer.newLine();
            }
        }
        
        logger.info("Generated Yellow taxi CSV with 1000 records at: {}", csvFilePath);
        return csvFilePath;
    }
    
    /**
     * Generate Green taxi CSV sample data
     */
    private String generateGreenTaxiCsv() throws IOException {
        String csvFilePath = dataDirectory + "/sample-nyc-green-taxi.csv";
        File csvFile = new File(csvFilePath);
        csvFile.getParentFile().mkdirs();
        
        Random random = new Random(43); // Different seed for Green taxi
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String[] boroughs = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"};
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
            // Write header (Green taxi has slightly different schema)
            writer.write("vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code_id,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,pickup_location_id,dropoff_location_id,pickup_borough,dropoff_borough");
            writer.newLine();
            
            // Generate sample records (500 records for demo)
            for (int i = 0; i < 500; i++) {
                LocalDateTime pickupTime = LocalDateTime.of(2024, 
                    random.nextInt(12) + 1,
                    random.nextInt(28) + 1,
                    random.nextInt(24),
                    random.nextInt(60));
                
                LocalDateTime dropoffTime = pickupTime.plusMinutes(random.nextInt(120) + 5);
                
                int passengerCount = random.nextInt(6) + 1;
                double tripDistance = 0.5 + random.nextDouble() * 15; // Slightly shorter trips for Green taxi
                
                // Green taxi operates mainly in outer boroughs
                double pickupLon = -74.2 + random.nextDouble() * 0.5; // Wider range
                double pickupLat = 40.6 + random.nextDouble() * 0.5;
                double dropoffLon = -74.2 + random.nextDouble() * 0.5;
                double dropoffLat = 40.6 + random.nextDouble() * 0.5;
                
                int rateCodeId = random.nextInt(6) + 1;
                String storeAndFwd = random.nextBoolean() ? "Y" : "N";
                int paymentType = random.nextInt(4) + 1;
                
                double fareAmount = 2.5 + tripDistance * 2.5 + random.nextDouble() * 3;
                double extra = random.nextBoolean() ? 0.5 : 0.0;
                double mtaTax = 0.5;
                double tipAmount = random.nextDouble() * fareAmount * 0.15; // Slightly lower tips
                double tollsAmount = random.nextBoolean() ? random.nextDouble() * 8 : 0.0;
                double improvementSurcharge = 0.3; // Green taxi surcharge
                double totalAmount = fareAmount + extra + mtaTax + tipAmount + tollsAmount + improvementSurcharge;
                
                int pickupLocationId = random.nextInt(265) + 1;
                int dropoffLocationId = random.nextInt(265) + 1;
                String pickupBorough = boroughs[random.nextInt(boroughs.length)];
                String dropoffBorough = boroughs[random.nextInt(boroughs.length)];
                
                writer.write(String.format("%d,%s,%s,%d,%.2f,%.6f,%.6f,%d,%s,%.6f,%.6f,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%s,%s",
                    random.nextInt(2) + 1,
                    pickupTime.format(formatter),
                    dropoffTime.format(formatter),
                    passengerCount,
                    tripDistance,
                    pickupLon,
                    pickupLat,
                    rateCodeId,
                    storeAndFwd,
                    dropoffLon,
                    dropoffLat,
                    paymentType,
                    fareAmount,
                    extra,
                    mtaTax,
                    tipAmount,
                    tollsAmount,
                    improvementSurcharge,
                    totalAmount,
                    pickupLocationId,
                    dropoffLocationId,
                    pickupBorough,
                    dropoffBorough));
                writer.newLine();
            }
        }
        
        logger.info("Generated Green taxi CSV with 500 records at: {}", csvFilePath);
        return csvFilePath;
    }
    
    /**
     * Get current loading status
     */
    public String getLoadingStatus() {
        return loadingStatus;
    }
    
    /**
     * Get taxi data statistics
     */
    public TaxiDataStats getDataStats() {
        // This would typically query the actual data
        return new TaxiDataStats(1000, 500, 1500);
    }
    
    /**
     * Taxi data statistics
     */
    public static class TaxiDataStats {
        private final long yellowTaxiRecords;
        private final long greenTaxiRecords;
        private final long totalRecords;
        
        public TaxiDataStats(long yellowTaxiRecords, long greenTaxiRecords, long totalRecords) {
            this.yellowTaxiRecords = yellowTaxiRecords;
            this.greenTaxiRecords = greenTaxiRecords;
            this.totalRecords = totalRecords;
        }
        
        public long getYellowTaxiRecords() { return yellowTaxiRecords; }
        public long getGreenTaxiRecords() { return greenTaxiRecords; }
        public long getTotalRecords() { return totalRecords; }
    }
}