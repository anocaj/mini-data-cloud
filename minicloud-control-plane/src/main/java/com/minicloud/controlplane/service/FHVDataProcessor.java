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
import java.util.Random;

/**
 * Processes NYC For-Hire Vehicle (FHV) data including Uber, Lyft, and other ride-sharing services.
 * Implements time-based partitioning and dispatching base analysis.
 */
@Service
public class FHVDataProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(FHVDataProcessor.class);
    
    @Autowired
    private DataLoadingService dataLoadingService;
    
    @Autowired
    private PartitioningStrategy partitioningStrategy;
    
    @Value("${minicloud.storage.data-directory:./data}")
    private String dataDirectory;
    
    private volatile String loadingStatus = "NOT_STARTED";
    
    // Common FHV dispatching bases (simplified for demo)
    private static final String[] DISPATCHING_BASES = {
        "B02512", // Uber
        "B02598", // Uber
        "B02617", // Uber
        "B02682", // Uber
        "B02764", // Lyft
        "B02765", // Lyft
        "B02835", // Lyft
        "B02836", // Lyft
        "B02510", // Via
        "B02395", // Juno
        "B02404", // Other
        "B02416", // Other
        "B02422", // Other
        "B02480", // Other
        "B02555"  // Other
    };
    
    // Affiliated base numbers
    private static final String[] AFFILIATED_BASES = {
        "B02512", "B02598", "B02617", "B02682", "B02764", "B02765", "B02835", "B02836"
    };
    
    /**
     * Load sample FHV data
     */
    public void loadSampleFHVData() {
        logger.info("Starting to load sample NYC FHV data...");
        loadingStatus = "LOADING";
        
        try {
            // Generate and load FHV data
            String csvFilePath = generateFHVCsv();
            
            DataLoadingService.LoadResult result = dataLoadingService.loadCsvData(
                csvFilePath, "nyc", "fhv_trips", true);
            
            logger.info("Loaded FHV data: {} rows", result.getRowCount());
            
            // Apply intelligent partitioning
            partitioningStrategy.applyFHVTripPartitioning("nyc", "fhv_trips");
            
            loadingStatus = "COMPLETED";
            logger.info("Successfully loaded NYC FHV sample data");
            
        } catch (Exception e) {
            loadingStatus = "FAILED: " + e.getMessage();
            logger.error("Failed to load NYC FHV data", e);
            throw new RuntimeException("Failed to load FHV data", e);
        }
    }
    
    /**
     * Generate FHV CSV sample data
     */
    private String generateFHVCsv() throws IOException {
        String csvFilePath = dataDirectory + "/sample-nyc-fhv.csv";
        File csvFile = new File(csvFilePath);
        csvFile.getParentFile().mkdirs();
        
        Random random = new Random(45); // Fixed seed for reproducible data
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
            // Write header
            writer.write("dispatching_base_num,pickup_datetime,dropoff_datetime,pickup_location_id,dropoff_location_id,sr_flag,affiliated_base_number");
            writer.newLine();
            
            // Generate sample records (1500 records for demo)
            for (int i = 0; i < 1500; i++) {
                String dispatchingBaseNum = DISPATCHING_BASES[random.nextInt(DISPATCHING_BASES.length)];
                
                LocalDateTime pickupTime = LocalDateTime.of(2024, 
                    random.nextInt(12) + 1,
                    random.nextInt(28) + 1,
                    random.nextInt(24),
                    random.nextInt(60));
                
                LocalDateTime dropoffTime = pickupTime.plusMinutes(random.nextInt(90) + 10); // 10-100 minutes trip
                
                int pickupLocationId = random.nextInt(265) + 1; // NYC taxi zones 1-265
                int dropoffLocationId = random.nextInt(265) + 1;
                
                // SR flag (Shared Ride) - some trips are shared
                String srFlag = random.nextDouble() < 0.15 ? "Y" : "N"; // 15% shared rides
                
                // Affiliated base number (for some dispatching bases)
                String affiliatedBaseNumber = "";
                if (random.nextBoolean() && isAffiliatedBase(dispatchingBaseNum)) {
                    affiliatedBaseNumber = AFFILIATED_BASES[random.nextInt(AFFILIATED_BASES.length)];
                }
                
                writer.write(String.format("%s,%s,%s,%d,%d,%s,%s",
                    dispatchingBaseNum,
                    pickupTime.format(formatter),
                    dropoffTime.format(formatter),
                    pickupLocationId,
                    dropoffLocationId,
                    srFlag,
                    affiliatedBaseNumber));
                writer.newLine();
            }
        }
        
        logger.info("Generated FHV CSV with 1500 records at: {}", csvFilePath);
        return csvFilePath;
    }
    
    /**
     * Check if a dispatching base is affiliated with a larger company
     */
    private boolean isAffiliatedBase(String dispatchingBaseNum) {
        for (String affiliatedBase : AFFILIATED_BASES) {
            if (affiliatedBase.equals(dispatchingBaseNum)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get current loading status
     */
    public String getLoadingStatus() {
        return loadingStatus;
    }
    
    /**
     * Get FHV data statistics
     */
    public FHVDataStats getDataStats() {
        // This would typically query the actual data
        return new FHVDataStats(1500, 225, 1275, 15);
    }
    
    /**
     * Get dispatching base analysis
     */
    public DispatchingBaseAnalysis getDispatchingBaseAnalysis() {
        // This would typically query the actual data for real analysis
        return new DispatchingBaseAnalysis(
            "B02512", // Most active base (Uber)
            450,      // Trip count for most active
            15,       // Total number of bases
            0.15      // Shared ride percentage
        );
    }
    
    /**
     * FHV data statistics
     */
    public static class FHVDataStats {
        private final long totalTrips;
        private final long sharedRideTrips;
        private final long regularTrips;
        private final int uniqueBases;
        
        public FHVDataStats(long totalTrips, long sharedRideTrips, long regularTrips, int uniqueBases) {
            this.totalTrips = totalTrips;
            this.sharedRideTrips = sharedRideTrips;
            this.regularTrips = regularTrips;
            this.uniqueBases = uniqueBases;
        }
        
        public long getTotalTrips() { return totalTrips; }
        public long getSharedRideTrips() { return sharedRideTrips; }
        public long getRegularTrips() { return regularTrips; }
        public int getUniqueBases() { return uniqueBases; }
    }
    
    /**
     * Dispatching base analysis
     */
    public static class DispatchingBaseAnalysis {
        private final String mostActiveBase;
        private final long mostActiveBaseTrips;
        private final int totalBases;
        private final double sharedRidePercentage;
        
        public DispatchingBaseAnalysis(String mostActiveBase, long mostActiveBaseTrips, 
                                     int totalBases, double sharedRidePercentage) {
            this.mostActiveBase = mostActiveBase;
            this.mostActiveBaseTrips = mostActiveBaseTrips;
            this.totalBases = totalBases;
            this.sharedRidePercentage = sharedRidePercentage;
        }
        
        public String getMostActiveBase() { return mostActiveBase; }
        public long getMostActiveBaseTrips() { return mostActiveBaseTrips; }
        public int getTotalBases() { return totalBases; }
        public double getSharedRidePercentage() { return sharedRidePercentage; }
    }
}