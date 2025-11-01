package com.minicloud.controlplane.sql;

import com.minicloud.controlplane.service.GlobalCatalogService;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

/**
 * Service for parsing and validating SQL queries using Apache Calcite
 */
@Service
public class SqlParsingService {
    
    private static final Logger logger = LoggerFactory.getLogger(SqlParsingService.class);
    
    private final FrameworkConfig frameworkConfig;
    private final SqlValidator sqlValidator;
    private final GlobalCatalogService catalogService;
    
    @Autowired
    public SqlParsingService(FrameworkConfig frameworkConfig, SqlValidator sqlValidator, 
                           GlobalCatalogService catalogService) {
        this.frameworkConfig = frameworkConfig;
        this.sqlValidator = sqlValidator;
        this.catalogService = catalogService;
        logger.info("SQL parsing service initialized with Calcite framework and Iceberg catalog support");
    }
    
    /**
     * Parse SQL string into SqlNode AST
     */
    public SqlNode parseQuery(String sql) throws SqlParseException {
        logger.debug("Parsing SQL query: {}", sql.substring(0, Math.min(sql.length(), 100)) + "...");
        
        SqlParser parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
        SqlNode sqlNode = parser.parseQuery();
        
        logger.debug("Successfully parsed SQL into AST: {}", sqlNode.getKind());
        return sqlNode;
    }
    
    /**
     * Validate parsed SQL query
     */
    public SqlNode validateQuery(SqlNode sqlNode) throws ValidationException {
        logger.debug("Validating SQL query: {}", sqlNode.getKind());
        
        try {
            SqlNode validatedNode = sqlValidator.validate(sqlNode);
            logger.debug("SQL query validation successful");
            return validatedNode;
        } catch (Exception e) {
            logger.error("SQL validation failed: {}", e.getMessage());
            throw new ValidationException("SQL validation failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Convert validated SQL to RelNode tree
     */
    public RelNode convertToRelNode(SqlNode validatedSqlNode) throws RelConversionException {
        logger.debug("Converting SQL to RelNode tree");
        
        try (Planner planner = Frameworks.getPlanner(frameworkConfig)) {
            SqlNode parsedQuery = planner.parse(validatedSqlNode.toString());
            SqlNode validatedQuery = planner.validate(parsedQuery);
            RelRoot relRoot = planner.rel(validatedQuery);
            
            RelNode relNode = relRoot.rel;
            logger.debug("Successfully converted to RelNode: {}", relNode.getRelTypeName());
            logger.trace("RelNode tree: {}", RelOptUtil.toString(relNode));
            
            return relNode;
        } catch (Exception e) {
            logger.error("Failed to convert SQL to RelNode: {}", e.getMessage());
            throw new RelConversionException("Failed to convert SQL to RelNode: " + e.getMessage(), e);
        }
    }
    
    /**
     * Complete SQL parsing pipeline: parse -> validate -> convert to RelNode
     */
    public ParsedQuery parseAndValidateQuery(String sql) throws SqlParsingException {
        try {
            // Step 1: Parse SQL string to AST
            SqlNode sqlNode = parseQuery(sql);
            
            // Step 2: Validate the parsed query
            SqlNode validatedNode = validateQuery(sqlNode);
            
            // Step 3: Convert to RelNode tree
            RelNode relNode = convertToRelNode(validatedNode);
            
            logger.info("Successfully parsed and validated SQL query");
            return new ParsedQuery(sql, sqlNode, validatedNode, relNode);
            
        } catch (SqlParseException e) {
            logger.error("SQL parsing failed for query: {} - Error: {}", sql, e.getMessage());
            throw new SqlParsingException("SQL parsing failed: " + e.getMessage(), e);
        } catch (ValidationException e) {
            logger.error("SQL validation failed for query: {} - Error: {}", sql, e.getMessage());
            throw new SqlParsingException("SQL validation failed: " + e.getMessage(), e);
        } catch (RelConversionException e) {
            logger.error("RelNode conversion failed for query: {} - Error: {}", sql, e.getMessage());
            throw new SqlParsingException("RelNode conversion failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Check if a SQL query is supported by our system
     */
    public boolean isQuerySupported(String sql) {
        try {
            SqlNode sqlNode = parseQuery(sql);
            return SqlFeatureValidator.isSupported(sqlNode);
        } catch (Exception e) {
            logger.debug("Query not supported due to parsing error: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Get detailed information about why a query is not supported
     */
    public String getUnsupportedFeatureReason(String sql) {
        try {
            SqlNode sqlNode = parseQuery(sql);
            return SqlFeatureValidator.getUnsupportedFeatureReason(sqlNode);
        } catch (Exception e) {
            return "Query parsing failed: " + e.getMessage();
        }
    }
    
    /**
     * Checks if a table reference in the query is an Iceberg table.
     */
    public boolean isIcebergTable(String tableName) {
        if (catalogService == null || !catalogService.isEnabled()) {
            return false;
        }
        
        try {
            // Try to resolve as Iceberg table
            TableIdentifier tableId = TableIdentifier.of("default", tableName);
            return catalogService.tableExists(tableId);
        } catch (Exception e) {
            logger.debug("Table {} is not an Iceberg table: {}", tableName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Extracts table references from a parsed SQL query.
     */
    public Set<String> extractTableReferences(SqlNode sqlNode) {
        Set<String> tableNames = new HashSet<>();
        
        try {
            // Use a visitor to extract table names from the SQL AST
            SqlTableExtractor extractor = new SqlTableExtractor();
            sqlNode.accept(extractor);
            tableNames.addAll(extractor.getTableNames());
            
            logger.debug("Extracted table references: {}", tableNames);
        } catch (Exception e) {
            logger.warn("Failed to extract table references from SQL", e);
        }
        
        return tableNames;
    }
    
    /**
     * Identifies which tables in a query are Iceberg tables.
     */
    public Set<String> identifyIcebergTables(String sql) {
        Set<String> icebergTables = new HashSet<>();
        
        try {
            SqlNode sqlNode = parseQuery(sql);
            Set<String> allTables = extractTableReferences(sqlNode);
            
            for (String tableName : allTables) {
                if (isIcebergTable(tableName)) {
                    icebergTables.add(tableName);
                }
            }
            
            logger.debug("Identified Iceberg tables in query: {}", icebergTables);
        } catch (Exception e) {
            logger.warn("Failed to identify Iceberg tables in query", e);
        }
        
        return icebergTables;
    }
    
    /**
     * Simple visitor to extract table names from SQL AST.
     */
    private static class SqlTableExtractor extends org.apache.calcite.sql.util.SqlBasicVisitor<Void> {
        private final Set<String> tableNames = new HashSet<>();
        
        @Override
        public Void visit(org.apache.calcite.sql.SqlIdentifier id) {
            if (id.names.size() == 1) {
                // Simple table name
                tableNames.add(id.names.get(0));
            } else if (id.names.size() == 2) {
                // Schema.table format - use table name
                tableNames.add(id.names.get(1));
            }
            return null;
        }
        
        public Set<String> getTableNames() {
            return tableNames;
        }
    }
}