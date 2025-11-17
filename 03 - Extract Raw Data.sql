-- AI Extract Job Requisition Information - Two-Step Process
-- Step 1: Extract raw data from job descriptions and store in table (expensive operation - run once)
-- Step 2: Parse and structure data from raw table (can be run multiple times)

-- =============================================================================
-- STEP 1: EXTRACT RAW DATA FROM JOB DESCRIPTIONS AND STORE IN TABLE
-- =============================================================================
-- Run this first to perform the AI extraction and store results
-- This is the expensive operation that should only be run once per batch of job descriptions
-- REALLY IMPORTANT TO NOTE THAT IF YOU GAVE THE DIRECTORY THE NAME OF A REQUISITION THAT COULD BE YOUR "CATEGORY" AND YOU CAN DUMP RESUMES PER REQ 

CREATE OR REPLACE TABLE hackathon_2025.jobreqs.jobreq_extractions AS
SELECT 
  CURRENT_TIMESTAMP() as extraction_timestamp,
  REQ_ID,
  CATEGORY,
  AI_EXTRACT(TEXT => JOB_DESCRIPTION,
    responseFormat => {
      'job_1': 'What is the job title in the requisition?',
      'years_of_experience': 'How many total years of work experience does the position listed require? Provide just the number.',
      'technical_skills': 'List: What are the technical skills mentioned in the job requisition',
      'additional_skills': 'List: What are the additional non-technical skills mentioned in this job requisition?',
      'certifications': 'List: What certifications does this job requisition require?',
      'job_titles_list': 'What is the job title in the requisition?',
      'has_doctorate': 'Does this position require a doctorate degree? Answer true or false.',
      'has_masters': 'Does this position require a masters degree? Answer true or false.',
      'has_bachelors': 'Does this position require a bachelors degree? Answer true or false.',
      'has_high_school': 'Does this position require a high school diploma or equivalent? Answer true or false.',
      'doctorate_name': 'What is the name or field of the doctorate degree required?',
      'masters_name': 'What is the name or field of the masters degree required?',
      'bachelors_name': 'What is the name or field of the bachelors degree required?',
      'spoken_languages_bool': 'Does the position require non-english languages?',
      'spoken_languages': 'What spoken human languages does the position require?',
      'programming_languages': 'What programming languages does the position require?',
      'leadership_experience_bool': 'Does the positoin require any leadership experience?',
      'leadership_experience': 'What leadership experience does the position require?'
    }
  ) as extracted_info
FROM hackathon_2025.jobreqs.SILVER_JOB_LISTINGS_FLATTENED;

SELECT * FROM hackathon_2025.jobreqs.jobreq_extractions;

-- =============================================================================
-- STEP 2: PARSE AND STRUCTURE DATA FROM RAW TABLE
-- =============================================================================
-- Run this to get structured, parsed results from the raw extractions
-- This can be modified and re-run without re-doing the expensive AI extraction

CREATE OR REPLACE TABLE jobreq_flattened AS (
WITH parsed_data AS (
  SELECT 
    -- extraction_timestamp,
    REQ_ID,
    CATEGORY,
    extracted_info,
    extracted_info:response:job_1::STRING as job_title,
    CASE 
      WHEN LOWER(TRIM(extracted_info:response:years_of_experience::STRING)) IN ('none', 'no experience', 'n/a', 'null', '0') THEN 0
      WHEN REGEXP_LIKE(extracted_info:response:years_of_experience::STRING, '^\\d+\\+?$') THEN 
        REGEXP_REPLACE(extracted_info:response:years_of_experience::STRING, '\\+', '')::NUMBER
      WHEN REGEXP_LIKE(extracted_info:response:years_of_experience::STRING, '^\\d+') THEN 
        REGEXP_SUBSTR(extracted_info:response:years_of_experience::STRING, '^\\d+')::NUMBER
      ELSE 0
    END as years_of_experience_required,
    extracted_info:response:technical_skills::STRING as technical_skills_required,
    extracted_info:response:additional_skills::STRING as additional_skills_required,
    extracted_info:response:certifications::STRING as certifications_required,
    extracted_info:response:job_titles_list::STRING as job_titles_list,
    CASE 
      WHEN LOWER(extracted_info:response:has_doctorate::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as requires_doctorate,
    CASE 
      WHEN LOWER(extracted_info:response:has_masters::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as requires_masters,
    CASE 
      WHEN LOWER(extracted_info:response:has_bachelors::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as requires_bachelors,
    CASE 
      WHEN LOWER(extracted_info:response:has_high_school::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as requires_high_school_extracted,
    extracted_info:response:doctorate_name::STRING as doctorate_name_raw,
    extracted_info:response:masters_name::STRING as masters_name_raw,
    extracted_info:response:bachelors_name::STRING as bachelors_name_raw,
    CASE 
      WHEN LOWER(extracted_info:response:spoken_languages_bool::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as requires_non_english_languages,
    extracted_info:response:spoken_languages::STRING as spoken_languages_required,
    extracted_info:response:programming_languages::STRING as programming_languages_required,
    CASE 
      WHEN LOWER(extracted_info:response:leadership_experience_bool::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as requires_leadership_experience,
    extracted_info:response:leadership_experience::STRING as leadership_experience_details
  FROM hackathon_2025.jobreqs.jobreq_extractions
)

SELECT 
  -- Extraction metadata
  -- extraction_timestamp,
  
  -- Original job posting information (all columns from SILVER_JOB_LISTINGS_FLATTENED)
  
  -- Job title information
  REQ_ID,
  CATEGORY,
  job_title,
  years_of_experience_required,
  
  -- Skills and certifications
  technical_skills_required,
  additional_skills_required,
  certifications_required,
  job_titles_list,
  
  -- Language requirements
  requires_non_english_languages,
  CASE 
    WHEN requires_non_english_languages THEN spoken_languages_required 
    ELSE NULL 
  END as spoken_languages_required,
  programming_languages_required,
  
  -- Leadership experience
  requires_leadership_experience,
  CASE 
    WHEN requires_leadership_experience THEN leadership_experience_details 
    ELSE NULL 
  END as leadership_experience_details,
  
  -- Education boolean flags
  requires_doctorate,
  requires_masters,
  requires_bachelors,
  
  -- High school diploma logic: true if they require any higher education OR if explicitly mentioned
  CASE 
    WHEN requires_doctorate OR requires_masters OR requires_bachelors THEN TRUE
    ELSE requires_high_school_extracted
  END as requires_high_school_diploma,
  
  -- Education names - set to NULL if corresponding boolean is false
  CASE 
    WHEN requires_doctorate THEN doctorate_name_raw 
    ELSE NULL 
  END as required_doctorate_field,
  
  CASE 
    WHEN requires_masters THEN masters_name_raw 
    ELSE NULL 
  END as required_masters_field,
  
  CASE 
    WHEN requires_bachelors THEN bachelors_name_raw 
    ELSE NULL 
  END as required_bachelors_field,
  
  -- Full extracted JSON for reference
  extracted_info as full_extracted_json

FROM parsed_data)
;

SELECT * FROM JOBREQ_FLATTENED;

ALTER TABLE JOBREQ_FLATTENED 
ADD COLUMN vector_embedding_variant_jobreq VARIANT;

-- Update the table to populate the new vector_embedding_variant column
UPDATE JOBREQ_FLATTENED 
SET vector_embedding_variant_jobreq = OBJECT_CONSTRUCT(
    'job_1', JOB_TITLE,
    'years_of_experience', YEARS_OF_EXPERIENCE_REQUIRED,
    'technical_skills', TECHNICAL_SKILLS_REQUIRED,
    'additional_skills', ADDITIONAL_SKILLS_REQUIRED,
    'certifications', CERTIFICATIONS_REQUIRED,
    'job_titles_list', JOB_TITLES_LIST,
    'has_doctorate', REQUIRES_DOCTORATE,
    'has_masters', REQUIRES_MASTERS,
    'has_bachelors', REQUIRES_BACHELORS,
    'has_highschool', REQUIRES_HIGH_SCHOOL_DIPLOMA,
    'doctorate_name', REQUIRED_DOCTORATE_FIELD,
    'masters_name', REQUIRED_MASTERS_FIELD,
    'bachelors_name', REQUIRED_BACHELORS_FIELD,
    'spoken_languages_bool', REQUIRES_NON_ENGLISH_LANGUAGES,
    'spoken_languages', SPOKEN_LANGUAGES_REQUIRED,
    'programming_languages', PROGRAMMING_LANGUAGES_REQUIRED,
    'leadership_experience_bool', REQUIRES_LEADERSHIP_EXPERIENCE,
    'leadership_experience', LEADERSHIP_EXPERIENCE_DETAILS
);

SELECT * FROM JOBREQ_FLATTENED;


-- =============================================================================
-- INSTRUCTIONS FOR USE:
-- =============================================================================
-- 
-- TWO-STEP PROCESS:
-- 
-- STEP 1 - EXTRACT (Run Once):
--   - Run the CREATE OR REPLACE TABLE section above first
--   - This performs the expensive AI_EXTRACT operation and stores raw results
--   - Uses source table: hackathon_2025.jobreqs.SILVER_JOB_LISTINGS_FLATTENED
--   - Creates table: hackathon_2025.jobreqs.jobreq_extractions
-- 
-- STEP 2 - PARSE (Run Multiple Times):
--   - Run the SELECT query (with CTE) to get structured results
--   - This reads from jobreq_extractions table (fast operation)
--   - Can be modified and re-run without re-doing AI extraction
--   - Can create different views or analyses from the same raw data
-- 
-- REQUIREMENTS:
-- 1. Table 'hackathon_2025.jobreqs.SILVER_JOB_LISTINGS_FLATTENED' must exist and be accessible
-- 2. Ensure the table contains job description text in 'JOB_DESCRIPTION' column
-- 3. Make sure you have the appropriate permissions to access the table and use AI_EXTRACT
-- 4. Have CREATE TABLE permissions for the jobreq_extractions table
-- 
-- BENEFITS OF THIS APPROACH:
-- - AI extraction only runs once (expensive operation)
-- - Parsing logic can be modified and re-run quickly
-- - Raw JSON is preserved for future analysis
-- - Timestamps track when extraction was performed
-- - Can create multiple views from same raw data
-- - Maintains all original job posting columns alongside extracted data
-- 
-- FIELD NAMING CONVENTIONS:
-- - All extracted requirement fields use "requires_" or "_required" suffix for clarity
-- - Boolean fields clearly indicate what is required vs. optional
-- - Education fields specify the field/type of degree required
-- - Maintains consistency with resume extraction patterns for easy comparison