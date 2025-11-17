
use database HACKATHON_2025;
use schema JOBREQS;


-- 3. Load REQ Data from Stage to Bronze Table
-- ===============================================

COPY INTO JOBREQS.bronze_job_listings (filename, file_row_number, raw_json)
FROM (
    SELECT
        metadata$filename,
        metadata$file_row_number,
        $1
    FROM @JOBREQS.job_listings_stage
)
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
)
ON_ERROR = 'CONTINUE'  -- Continue loading even if some records fail
PURGE = FALSE;  -- Set to TRUE if you want to delete files after successful load
-- Check load results
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT filename) as unique_files,
    MIN(ingestion_timestamp) as first_load,
    MAX(ingestion_timestamp) as last_load
FROM JOBREQS.bronze_job_listings;

