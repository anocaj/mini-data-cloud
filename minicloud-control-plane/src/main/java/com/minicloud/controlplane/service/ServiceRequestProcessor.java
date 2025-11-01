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
 * Processes NYC 311 service request data with category-based partitioning.
 * Manages citizen service requests with intelligent partitioning by agency and complaint type.
 */
@Service
public class ServiceRequestProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(ServiceRequestProcessor.class);
    
    @Autowired
    private DataLoadingService dataLoadingService;
    
    @Autowired
    private PartitioningStrategy partitioningStrategy;
    
    @Value("${minicloud.storage.data-directory:./data}")
    private String dataDirectory;
    
    private volatile String loadingStatus = "NOT_STARTED";
    
    // Common NYC agencies and complaint types
    private static final String[] AGENCIES = {
        "NYPD", "FDNY", "DOT", "DEP", "DSNY", "HPD", "DOB", "DOHMH", "DPR", "DOE"
    };
    
    private static final String[] COMPLAINT_TYPES = {
        "Noise - Residential", "Heat/Hot Water", "Street Light Condition", "Water System",
        "Blocked Driveway", "Illegal Parking", "Traffic Signal Condition", "Street Condition",
        "Graffiti", "Sanitation Condition", "Animal Abuse", "Air Quality", "Taxi Complaint",
        "Building/Use", "Homeless Person Assistance", "Rodent", "Water Quality", "Sewer"
    };
    
    private static final String[] BOROUGHS = {
        "MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"
    };
    
    private static final String[] STATUSES = {
        "Open", "Closed", "Pending", "In Progress", "Assigned"
    };
    
    /**
     * Load sample 311 service request data
     */
    public void loadSample311Data() {
        logger.info("Starting to load sample NYC 311 service request data...");
        loadingStatus = "LOADING";
        
        try {
            // Generate and load 311 service request data
            String csvFilePath = generate311ServiceRequestCsv();
            
            DataLoadingService.LoadResult result = dataLoadingService.loadCsvData(
                csvFilePath, "nyc", "service_requests_311", true);
            
            logger.info("Loaded 311 service request data: {} rows", result.getRowCount());
            
            // Apply intelligent partitioning
            partitioningStrategy.apply311ServiceRequestPartitioning("nyc", "service_requests_311");
            
            loadingStatus = "COMPLETED";
            logger.info("Successfully loaded NYC 311 service request sample data");
            
        } catch (Exception e) {
            loadingStatus = "FAILED: " + e.getMessage();
            logger.error("Failed to load NYC 311 service request data", e);
            throw new RuntimeException("Failed to load 311 service request data", e);
        }
    }
    
    /**
     * Generate 311 service request CSV sample data
     */
    private String generate311ServiceRequestCsv() throws IOException {
        String csvFilePath = dataDirectory + "/sample-nyc-311-requests.csv";
        File csvFile = new File(csvFilePath);
        csvFile.getParentFile().mkdirs();
        
        Random random = new Random(44); // Fixed seed for reproducible data
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
            // Write header
            writer.write("unique_key,created_date,closed_date,agency,agency_name,complaint_type,descriptor,location_type,incident_zip,incident_address,street_name,cross_street_1,cross_street_2,intersection_street_1,intersection_street_2,address_type,city,landmark,facility_type,status,due_date,resolution_description,resolution_action_updated_date,community_board,bbl,borough,x_coordinate_state_plane,y_coordinate_state_plane,open_data_channel_type,park_facility_name,park_borough,vehicle_type,taxi_company_borough,taxi_pick_up_location,bridge_highway_name,bridge_highway_direction,road_ramp,bridge_highway_segment,latitude,longitude,location");
            writer.newLine();
            
            // Generate sample records (2000 records for demo)
            for (int i = 0; i < 2000; i++) {
                String uniqueKey = String.format("SR%08d", i + 1);
                
                LocalDateTime createdDate = LocalDateTime.of(2024, 
                    random.nextInt(12) + 1,
                    random.nextInt(28) + 1,
                    random.nextInt(24),
                    random.nextInt(60));
                
                // Some requests are closed, some are still open
                LocalDateTime closedDate = null;
                String status = STATUSES[random.nextInt(STATUSES.length)];
                if ("Closed".equals(status) && random.nextBoolean()) {
                    closedDate = createdDate.plusDays(random.nextInt(30) + 1);
                }
                
                String agency = AGENCIES[random.nextInt(AGENCIES.length)];
                String agencyName = getAgencyFullName(agency);
                String complaintType = COMPLAINT_TYPES[random.nextInt(COMPLAINT_TYPES.length)];
                String descriptor = generateDescriptor(complaintType, random);
                
                String borough = BOROUGHS[random.nextInt(BOROUGHS.length)];
                String incidentZip = generateZipCode(borough, random);
                String incidentAddress = generateAddress(random);
                
                // NYC coordinates (approximate)
                double latitude = 40.7 + random.nextDouble() * 0.3;
                double longitude = -74.0 + random.nextDouble() * 0.3;
                
                LocalDateTime dueDate = createdDate.plusDays(random.nextInt(14) + 1);
                
                String resolutionDescription = "";
                String resolutionActionDate = "";
                if ("Closed".equals(status)) {
                    resolutionDescription = generateResolutionDescription(complaintType, random);
                    resolutionActionDate = closedDate != null ? closedDate.format(formatter) : "";
                }
                
                String communityBoard = String.format("%02d %s", random.nextInt(20) + 1, borough.substring(0, 3));
                
                writer.write(String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%.6f,%.6f,\"(%.6f, %.6f)\"",
                    uniqueKey,
                    createdDate.format(formatter),
                    closedDate != null ? closedDate.format(formatter) : "",
                    agency,
                    agencyName,
                    complaintType,
                    descriptor,
                    "Street/Sidewalk", // location_type
                    incidentZip,
                    incidentAddress,
                    generateStreetName(random), // street_name
                    "", // cross_street_1
                    "", // cross_street_2
                    "", // intersection_street_1
                    "", // intersection_street_2
                    "ADDRESS", // address_type
                    getCityFromBorough(borough),
                    "", // landmark
                    "", // facility_type
                    status,
                    dueDate.format(formatter),
                    resolutionDescription,
                    resolutionActionDate,
                    communityBoard,
                    "", // bbl
                    borough,
                    String.valueOf(random.nextInt(1000000) + 900000), // x_coordinate_state_plane
                    String.valueOf(random.nextInt(1000000) + 100000), // y_coordinate_state_plane
                    "ONLINE", // open_data_channel_type
                    "", // park_facility_name
                    "", // park_borough
                    "", // vehicle_type
                    "", // taxi_company_borough
                    "", // taxi_pick_up_location
                    "", // bridge_highway_name
                    "", // bridge_highway_direction
                    "", // road_ramp
                    "", // bridge_highway_segment
                    latitude,
                    longitude,
                    String.format("(%.6f, %.6f)", latitude, longitude)
                ));
                writer.newLine();
            }
        }
        
        logger.info("Generated 311 service request CSV with 2000 records at: {}", csvFilePath);
        return csvFilePath;
    }
    
    private String getAgencyFullName(String agency) {
        switch (agency) {
            case "NYPD": return "New York City Police Department";
            case "FDNY": return "Fire Department of New York";
            case "DOT": return "Department of Transportation";
            case "DEP": return "Department of Environmental Protection";
            case "DSNY": return "Department of Sanitation";
            case "HPD": return "Department of Housing Preservation and Development";
            case "DOB": return "Department of Buildings";
            case "DOHMH": return "Department of Health and Mental Hygiene";
            case "DPR": return "Department of Parks and Recreation";
            case "DOE": return "Department of Education";
            default: return agency;
        }
    }
    
    private String generateDescriptor(String complaintType, Random random) {
        switch (complaintType) {
            case "Noise - Residential":
                return random.nextBoolean() ? "Loud Music/Party" : "Loud Television";
            case "Heat/Hot Water":
                return random.nextBoolean() ? "No Heat" : "No Hot Water";
            case "Street Light Condition":
                return random.nextBoolean() ? "Street Light Out" : "Street Light Dim";
            case "Water System":
                return random.nextBoolean() ? "Water Leak" : "Water Pressure";
            case "Blocked Driveway":
                return "Blocked Driveway";
            case "Illegal Parking":
                return random.nextBoolean() ? "Double Parked" : "Blocking Hydrant";
            default:
                return complaintType + " Issue";
        }
    }
    
    private String generateZipCode(String borough, Random random) {
        switch (borough) {
            case "MANHATTAN":
                return String.valueOf(10000 + random.nextInt(100));
            case "BROOKLYN":
                return String.valueOf(11200 + random.nextInt(100));
            case "QUEENS":
                return String.valueOf(11300 + random.nextInt(100));
            case "BRONX":
                return String.valueOf(10400 + random.nextInt(100));
            case "STATEN ISLAND":
                return String.valueOf(10300 + random.nextInt(20));
            default:
                return "10001";
        }
    }
    
    private String generateAddress(Random random) {
        return String.format("%d %s %s", 
            random.nextInt(9999) + 1,
            generateStreetName(random),
            random.nextBoolean() ? "Street" : "Avenue");
    }
    
    private String generateStreetName(Random random) {
        String[] streetNames = {
            "Broadway", "Park", "Main", "Oak", "Pine", "Maple", "Cedar", "Elm",
            "Washington", "Lincoln", "Madison", "Jefferson", "Franklin", "Jackson"
        };
        return streetNames[random.nextInt(streetNames.length)];
    }
    
    private String getCityFromBorough(String borough) {
        switch (borough) {
            case "MANHATTAN":
                return "NEW YORK";
            case "BROOKLYN":
                return "BROOKLYN";
            case "QUEENS":
                return "QUEENS";
            case "BRONX":
                return "BRONX";
            case "STATEN ISLAND":
                return "STATEN ISLAND";
            default:
                return "NEW YORK";
        }
    }
    
    private String generateResolutionDescription(String complaintType, Random random) {
        switch (complaintType) {
            case "Noise - Residential":
                return "Officer responded and spoke with complainant. Issue resolved.";
            case "Heat/Hot Water":
                return "Violation issued to building owner. Heat/hot water restored.";
            case "Street Light Condition":
                return "Street light repaired by DOT crew.";
            case "Water System":
                return "DEP crew responded and repaired water issue.";
            case "Blocked Driveway":
                return "Vehicle was ticketed and towed.";
            case "Illegal Parking":
                return "Parking summons issued.";
            default:
                return "Issue addressed by appropriate agency.";
        }
    }
    
    /**
     * Get current loading status
     */
    public String getLoadingStatus() {
        return loadingStatus;
    }
    
    /**
     * Get 311 service request statistics
     */
    public ServiceRequestStats getDataStats() {
        // This would typically query the actual data
        return new ServiceRequestStats(2000, 1200, 800);
    }
    
    /**
     * Service request statistics
     */
    public static class ServiceRequestStats {
        private final long totalRequests;
        private final long closedRequests;
        private final long openRequests;
        
        public ServiceRequestStats(long totalRequests, long closedRequests, long openRequests) {
            this.totalRequests = totalRequests;
            this.closedRequests = closedRequests;
            this.openRequests = openRequests;
        }
        
        public long getTotalRequests() { return totalRequests; }
        public long getClosedRequests() { return closedRequests; }
        public long getOpenRequests() { return openRequests; }
    }
}