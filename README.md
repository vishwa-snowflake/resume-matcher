# JSON to Snowflake Data Pipeline

This repository contains a complete data engineering solution for loading JSON files into Snowflake using a bronze-silver architecture with variant columns and dynamic tables. All objects are created in the `JOBREQS` schema.

## 📁 Project Structure

```
├── 01_create_stage_and_bronze_table.sql    # Stage and bronze layer setup
├── 02_create_dynamic_flattened_table.sql   # Silver layer dynamic table
├── 03_sample_queries_and_advanced_flattening.sql  # Query examples
├── 04_deployment_instructions_and_best_practices.sql  # Operations guide
└── JSON/JOBLISTING_HR                       # Sample JSON data
```

## 🏗️ Architecture Overview

```
JSON Files → Stage → Bronze Table (VARIANT) → Dynamic Table (Structured) → Analytics
                ↓
            JOBREQS Schema
```

### Bronze Layer
- **Schema**: `JOBREQS`
- **Table**: `JOBREQS.bronze_job_listings`
- **Stage**: `JOBREQS.job_listings_stage` (Internal Stage)
- **Purpose**: Raw JSON storage with metadata
- **Key Features**:
  - VARIANT column for JSON data
  - Metadata tracking (filename, row number, ingestion timestamp)
  - Clustering for performance
  - Internal staging for secure, managed file storage

### Silver Layer  
- **Table**: `JOBREQS.silver_job_listings_flattened` (Dynamic Table)
- **Purpose**: Structured, queryable data
- **Key Features**:
  - Auto-refreshes when bronze data changes
  - Flattened JSON into typed columns
  - Optimized for analytics queries

## 📋 Prerequisites

1. **Snowflake Database**: Ensure database exists (or use default)
2. **Schema Access**: Create or have access to `JOBREQS` schema  
3. **Warehouse**: Active compute warehouse (default: `COMPUTE_WH`)
4. **Permissions**: CREATE TABLE, CREATE STAGE, CREATE DYNAMIC TABLE privileges

```sql
-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS JOBREQS;
USE SCHEMA JOBREQS;
```

## 🚀 Quick Start

### 1. Deploy Infrastructure
```sql
-- Run in order:
\i 01_create_stage_and_bronze_table.sql
\i 02_create_dynamic_flattened_table.sql
```

### 2. Load Data
```sql
-- Upload JSON file to internal stage
PUT file:///Users/jderlein/TechUP/JSON/JOBLISTING_HR @JOBREQS.job_listings_stage;

-- Verify file upload
LIST @JOBREQS.job_listings_stage;

-- Load into bronze table (already in script 1)
COPY INTO JOBREQS.bronze_job_listings ...
```

### 3. Verify Pipeline
```sql
-- Check data counts
SELECT COUNT(*) FROM JOBREQS.bronze_job_listings;
SELECT COUNT(*) FROM JOBREQS.silver_job_listings_flattened;

-- Check dynamic table status
SHOW DYNAMIC TABLES IN SCHEMA JOBREQS;
```

## 📊 Key Features

### Internal Staging Benefits
- **Managed Storage**: Files encrypted and stored within Snowflake
- **No External Dependencies**: No need for S3/Azure/GCS configuration
- **Integrated Security**: Leverages Snowflake's built-in access controls
- **Simplified Workflow**: Direct file upload via PUT command
- **Cost Effective**: No additional cloud storage costs
- **Automatic Compression**: Files automatically compressed during upload

### Dynamic Table Benefits
- **Auto-Refresh**: Automatically updates when source data changes
- **Incremental**: Only processes new/changed data
- **Performance**: Optimized for analytical queries
- **Maintenance-Free**: No need to manage refresh schedules

### JSON Flattening
- **Core Fields**: job_id, title, organization, dates
- **Location Data**: Addresses, coordinates, derived locations
- **Employment Info**: Types, salary, seniority
- **Organization Details**: LinkedIn data, company metrics
- **Nested Arrays**: Proper handling of multiple values

### Advanced Queries
- **Location Explosion**: Multiple locations per job
- **Employment Type Analysis**: Array element querying  
- **Data Quality Checks**: Built-in validation
- **Business Intelligence**: Ready-to-use analytics queries

## 🔍 Sample Data Structure

The JSON contains job listings with structure like:
```json
{
  "id": "1871603589",
  "title": "HR Coordinator", 
  "organization": "Thread HCM",
  "date_posted": "2025-09-17T14:24:17",
  "locations_raw": [
    {
      "address": {
        "addressCountry": "US",
        "addressLocality": "Alpharetta"
      },
      "latitude": 34.075615,
      "longitude": -84.29455
    }
  ],
  "employment_type": ["FULL_TIME"],
  "linkedin_org_employees": 66
}
```

## 📈 Performance Optimizations

- **Clustering**: Bronze table clustered by ingestion_timestamp
- **Search Optimization**: Enabled on silver table
- **Automatic Indexing**: Snowflake's automatic optimization
- **Result Caching**: Enabled for repeated queries
- **Materialized Views**: For frequently accessed aggregations

## 🛠️ Operations & Monitoring

### Data Quality Checks
```sql
-- Run automated checks
CALL JOBREQS.run_data_quality_checks();

-- View results
SELECT * FROM JOBREQS.data_quality_log ORDER BY check_timestamp DESC;
```

### Dynamic Table Monitoring
```sql
-- Check refresh status
SELECT name, behind, last_success_run, error_message
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name = 'SILVER_JOB_LISTINGS_FLATTENED' 
  AND schema_name = 'JOBREQS';
```

### Performance Monitoring
```sql
-- Query performance
SELECT query_text, execution_time, warehouse_name
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text LIKE '%JOBREQS.silver_job_listings_flattened%'
ORDER BY start_time DESC;
```

## 🔐 Security & Governance

### Role-Based Access
- **Data Engineers**: Full access to all layers
- **Data Analysts**: Read access to silver layer
- **Business Users**: Access to materialized views only

### Data Retention
- **Bronze Layer**: 2 years (configurable)
- **Silver Layer**: Follows bronze retention
- **Archive Strategy**: Quarterly archival process

## 🚨 Troubleshooting

### Common Issues

1. **Dynamic Table Not Refreshing**
   - Check warehouse status
   - Verify target lag settings
   - Review error messages in refresh history

2. **JSON Parsing Errors**
   - Validate JSON format in bronze table
   - Check for malformed records
   - Review COPY command error logs

3. **Performance Issues**
   - Increase warehouse size temporarily
   - Enable result caching
   - Check clustering statistics

## 📝 Next Steps

1. **Add Data Validation**: Implement comprehensive data quality rules
2. **Set Up Alerting**: Monitor pipeline health and failures  
3. **Expand Analytics**: Create domain-specific data marts
4. **Optimize Costs**: Implement tiered storage and compute strategies
5. **Add Testing**: Unit tests for transformations

## 🤝 Contributing

1. Test changes in development environment
2. Update documentation for new features
3. Run data quality checks before deployment
4. Follow SQL coding standards

## 📞 Support

For issues or questions:
- Check troubleshooting section in `04_deployment_instructions_and_best_practices.sql`
- Review Snowflake documentation for dynamic tables
- Contact data engineering team

---

**Last Updated**: September 17, 2025  
**Snowflake Version**: Compatible with all current versions  
**Dependencies**: Snowflake warehouse with appropriate permissions
