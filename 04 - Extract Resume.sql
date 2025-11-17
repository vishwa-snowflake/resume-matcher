-- AI Extract Resume Information from PDF Files in Stage - Two-Step Process
-- Step 1: Extract raw data from PDFs and store in table (expensive operation - run once)
-- Step 2: Parse and structure data from raw table (can be run multiple times)

-- =============================================================================
-- STEP 1: EXTRACT RAW DATA FROM PDFs AND STORE IN TABLE
-- =============================================================================
-- Run this first to perform the AI extraction and store results
-- This is the expensive operation that should only be run once per batch of files
-- REALLY IMPORTANT TO NOTE THAT IF YOU GAVE THE DIRECTORY THE NAME OF A REQUISITION THAT COULD BE YOUR "CATEGORY" AND YOU CAN DUMP RESUMES PER REQ 

CREATE OR REPLACE TABLE resume_pdf_extractions AS
SELECT 
  relative_path,
  file_url,
  CURRENT_TIMESTAMP() as extraction_timestamp,
  SPLIT_PART(relative_path, '/', 1) as category,
  SPLIT_PART(relative_path, '/', 2) as resume_id,
  AI_EXTRACT(
    file => TO_FILE('@hackathon_2025.joe.reqs_resumes', relative_path),
    responseFormat => {
      'job_1': 'What is the most recent or current job title of this person?',
      'years_of_experience': 'How many total years of work experience does this person have? Provide just the number.',
      'technical_skills': 'List: What are the technical skills mentioned in this resume?',
      'additional_skills': 'List: What are the additional non-technical skills mentioned in this resume?',
      'certifications': 'List: What certifications does this person have?',
      'job_titles_list': 'List: What are all the job titles this person has held?',
      'has_doctorate': 'Does this person have a doctorate degree? Answer true or false.',
      'has_masters': 'Does this person have a masters degree? Answer true or false.',
      'has_bachelors': 'Does this person have a bachelors degree? Answer true or false.',
      'has_high_school': 'Does this person have a high school diploma or equivalent? Answer true or false.',
      'doctorate_name': 'What is the name or field of the doctorate degree?',
      'masters_name': 'What is the name or field of the masters degree?',
      'bachelors_name': 'What is the name or field of the bachelors degree?',
      'community_volunteer_bool': 'Has the person done any volunteering or community work?',
      'community_volunteer': 'What volunteering or community work has the person done?',
      'spoken_languages_bool': 'Does the person speak non-english languages?',
      'spoken_languages': 'What spoken human languages does the person speak?',
      'programming_languages': 'What programming languages can the person write?',
      'college_sports': 'Did the candidate play sports in college?',
      'college_sports_division': 'If the person played college sports, what division did they play? If they did not play sports answer NULL',
      'personal_interests': 'What personal interests does the candidate have?',
      'Interesting': 'Was there anything interesting or abnormal listed in the individuals resume?',
      'leadership_experience_bool': 'Does the person have any leadership experience?',
      'leadership_experience': 'What leadership experience does the person have?'
    }
  ) as extracted_info
FROM DIRECTORY (@hackathon_2025.joe.reqs_resumes)
WHERE UPPER(relative_path) LIKE '%.PDF';  -- Only process PDF files

SELECT * from resume_pdf_extractions;

-- =============================================================================
-- STEP 2: PARSE AND STRUCTURE DATA FROM RAW TABLE
-- =============================================================================
-- Run this to get structured, parsed results from the raw extractions
-- This can be modified and re-run without re-doing the expensive AI extraction

CREATE OR REPLACE TABLE FLATTENED_RESUME_PDFS AS (
  WITH parsed_data AS (
    SELECT 
    relative_path,
    file_url,
    resume_id,
    extraction_timestamp,
    category,
    extracted_info,
    extracted_info:response:job_1::STRING as job_1,
    CASE 
      WHEN LOWER(TRIM(extracted_info:response:years_of_experience::STRING)) IN ('none', 'no experience', 'n/a', 'null', '0') THEN 0
      WHEN REGEXP_LIKE(extracted_info:response:years_of_experience::STRING, '^\\d+\\+?$') THEN 
        REGEXP_REPLACE(extracted_info:response:years_of_experience::STRING, '\\+', '')::NUMBER
      WHEN REGEXP_LIKE(extracted_info:response:years_of_experience::STRING, '^\\d+') THEN 
        REGEXP_SUBSTR(extracted_info:response:years_of_experience::STRING, '^\\d+')::NUMBER
      ELSE 0
    END as years_of_experience,
    extracted_info:response:technical_skills::STRING as technical_skills,
    extracted_info:response:additional_skills::STRING as additional_skills,
    extracted_info:response:certifications::STRING as certifications,
    extracted_info:response:job_titles_list::STRING as job_titles_list,
    CASE 
      WHEN LOWER(extracted_info:response:has_doctorate::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as has_doctorate,
    CASE 
      WHEN LOWER(extracted_info:response:has_masters::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as has_masters,
    CASE 
      WHEN LOWER(extracted_info:response:has_bachelors::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as has_bachelors,
    CASE 
      WHEN LOWER(extracted_info:response:has_high_school::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as has_high_school_extracted,
    extracted_info:response:doctorate_name::STRING as doctorate_name_raw,
    extracted_info:response:masters_name::STRING as masters_name_raw,
    extracted_info:response:bachelors_name::STRING as bachelors_name_raw,
    CASE 
      WHEN LOWER(extracted_info:response:community_volunteer_bool::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as community_volunteer_bool,
    extracted_info:response:community_volunteer::STRING as community_volunteer,
    CASE 
      WHEN LOWER(extracted_info:response:spoken_languages_bool::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as spoken_languages_bool,
    extracted_info:response:spoken_languages::STRING as spoken_languages,
    extracted_info:response:programming_languages::STRING as programming_languages,
    extracted_info:response:college_sports::STRING as college_sports,
    extracted_info:response:college_sports_division::STRING as college_sports_division,
    extracted_info:response:personal_interests::STRING as personal_interests,
    extracted_info:response:Interesting::STRING as interesting,
    CASE 
      WHEN LOWER(extracted_info:response:leadership_experience_bool::STRING) IN ('true', 'yes', '1') THEN TRUE 
      ELSE FALSE 
    END as leadership_experience_bool,
    extracted_info:response:leadership_experience::STRING as leadership_experience
  FROM resume_pdf_extractions
)

SELECT 
  -- File information
  relative_path as file_path,
  resume_id,
  file_url,
  extraction_timestamp,
  category,
  
  -- Job information
  job_1 as most_recent_job_title,
  years_of_experience,
  technical_skills,
  additional_skills,
  certifications,
  job_titles_list,
  
  -- Additional information
  community_volunteer_bool,
  CASE 
    WHEN community_volunteer_bool THEN community_volunteer 
    ELSE NULL 
  END as community_volunteer,
  spoken_languages_bool,
  CASE 
    WHEN spoken_languages_bool THEN spoken_languages 
    ELSE NULL 
  END as spoken_languages,
  programming_languages,
  
  -- Sports and interests
  college_sports,
  college_sports_division,
  personal_interests,
  interesting,
  
  -- Leadership experience
  leadership_experience_bool,
  CASE 
    WHEN leadership_experience_bool THEN leadership_experience 
    ELSE NULL 
  END as leadership_experience,
  
  -- Education boolean flags
  has_doctorate,
  has_masters,
  has_bachelors,
  
  -- High school diploma logic: true if they have any higher education OR if explicitly mentioned
  CASE 
    WHEN has_doctorate OR has_masters OR has_bachelors THEN TRUE
    ELSE has_high_school_extracted
  END as has_high_school_diploma,
  
  -- Education names - set to NULL if corresponding boolean is false
  CASE 
    WHEN has_doctorate THEN doctorate_name_raw 
    ELSE NULL 
  END as doctorate_name,
  
  CASE 
    WHEN has_masters THEN masters_name_raw 
    ELSE NULL 
  END as masters_name,
  
  CASE 
    WHEN has_bachelors THEN bachelors_name_raw 
    ELSE NULL 
  END as bachelors_name,
  
  -- Full extracted JSON for reference
  extracted_info as full_extracted_json

FROM parsed_data
ORDER BY relative_path);

SELECT * FROM FLATTENED_RESUME_PDFS;

SELECT * FROM HACKATHON_2025.JOBREQS.JOBREQ_EXTRACTIONS;

SELECT GET_DDL('TABLE','HACKATHON_2025.JOE.FLATTENED_RESUME_PDFS');

-- =============================================================================
-- INSTRUCTIONS FOR USE:
-- =============================================================================
-- 
-- TWO-STEP PROCESS:
-- 
-- STEP 1 - EXTRACT (Run Once):
--   - Run the CREATE OR REPLACE TABLE section above first
--   - This performs the expensive AI_EXTRACT operation and stores raw results
--   - Uses stage: '@hackathon_2025.joe.resume_pdfs'
--   - Creates table: resume_pdf_extractions
-- 
-- STEP 2 - PARSE (Run Multiple Times):
--   - Run the SELECT query (with CTE) to get structured results
--   - This reads from resume_pdf_extractions table (fast operation)
--   - Can be modified and re-run without re-doing AI extraction
--   - Can create different views or analyses from the same raw data
-- 
-- REQUIREMENTS:
-- 1. Stage '@hackathon_2025.joe.resume_pdfs' must be configured and accessible
-- 2. Ensure your stage contains PDF files with resumes
-- 3. Make sure you have the appropriate permissions to access the stage and use AI_EXTRACT
-- 4. Have CREATE TABLE permissions for the resume_pdf_extractions table
-- 
-- BENEFITS OF THIS APPROACH:
-- - AI extraction only runs once (expensive operation)
-- - Parsing logic can be modified and re-run quickly
-- - Raw JSON is preserved for future analysis
-- - Timestamps track when extraction was performed
-- - Can create multiple views from same raw data

-- Add vector_embedding_variant column to FLATTENED_RESUME_PDFS table
-- This creates an object with key-value pairs for the specified fields

ALTER TABLE FLATTENED_RESUME_PDFS 
ADD COLUMN vector_embedding_variant VARIANT;

-- Update the table to populate the new vector_embedding_variant column
UPDATE FLATTENED_RESUME_PDFS 
SET vector_embedding_variant = OBJECT_CONSTRUCT(
    'job_1', MOST_RECENT_JOB_TITLE,
    'years_of_experience', YEARS_OF_EXPERIENCE,
    'technical_skills', TECHNICAL_SKILLS,
    'additional_skills', ADDITIONAL_SKILLS,
    'certifications', CERTIFICATIONS,
    'job_titles_list', JOB_TITLES_LIST,
    'has_doctorate', HAS_DOCTORATE,
    'has_masters', HAS_MASTERS,
    'has_bachelors', HAS_BACHELORS,
    'has_highschool', HAS_HIGH_SCHOOL_DIPLOMA,
    'doctorate_name', DOCTORATE_NAME,
    'masters_name', MASTERS_NAME,
    'bachelors_name', BACHELORS_NAME,
    'spoken_languages_bool', SPOKEN_LANGUAGES_BOOL,
    'spoken_languages', SPOKEN_LANGUAGES,
    'programming_languages', PROGRAMMING_LANGUAGES,
    'leadership_experience_bool', LEADERSHIP_EXPERIENCE_BOOL,
    'leadership_experience', LEADERSHIP_EXPERIENCE
);

SELECT * FROM FLATTENED_RESUME_PDFS;



-- Alternative: If you want to create a new table with the vector_embedding_variant column
-- instead of altering the existing table, use this approach:

/*
CREATE OR REPLACE TABLE FLATTENED_RESUME_PDFS_WITH_VECTOR AS
SELECT 
    *,
    OBJECT_CONSTRUCT(
        'job_1', MOST_RECENT_JOB_TITLE,
        'years_of_experience', YEARS_OF_EXPERIENCE,
        'technical_skills', TECHNICAL_SKILLS,
        'additional_skills', ADDITIONAL_SKILLS,
        'certifications', CERTIFICATIONS,
        'job_titles_list', JOB_TITLES_LIST,
        'has_doctorate', HAS_DOCTORATE,
        'has_masters', HAS_MASTERS,
        'has_bachelors', HAS_BACHELORS,
        'has_highschool', HAS_HIGH_SCHOOL_DIPLOMA,
        'doctorate_name', DOCTORATE_NAME,
        'masters_name', MASTERS_NAME,
        'bachelors_name', BACHELORS_NAME,
        'spoken_languages_bool', SPOKEN_LANGUAGES_BOOL,
        'spoken_languages', SPOKEN_LANGUAGES,
        'programming_languages', PROGRAMMING_LANGUAGES,
        'leadership_experience_bool', LEADERSHIP_EXPERIENCE_BOOL,
        'leadership_experience', LEADERSHIP_EXPERIENCE
    ) AS vector_embedding_variant
FROM FLATTENED_RESUME_PDFS;
*/

-- Verify the results
SELECT 
    FILE_PATH,
    vector_embedding_variant
FROM FLATTENED_RESUME_PDFS 
LIMIT 5;

-- Optional: View the structure of the created object
SELECT 
    FILE_PATH,
    vector_embedding_variant,
    -- Access individual keys from the object
    vector_embedding_variant:job_1::STRING as job_1,
    vector_embedding_variant:years_of_experience::NUMBER as years_of_experience,
    vector_embedding_variant:has_doctorate::BOOLEAN as has_doctorate
FROM FLATTENED_RESUME_PDFS 
LIMIT 5;

SELECT * FROM FLATTENED_RESUME_PDFS;