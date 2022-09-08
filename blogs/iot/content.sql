/*
This template walks you through transforming raw data in your own S3 bucket and storing the results, fully optimized in your data lake to be queried with Athena.
Note: Do a global find/replace on <DB_NAME>, and replace it with your database name in your default glue catalog   

    1. Create and ingest into a staging table to store the raw data before transformation.
    2. Create a intermediate Materialized view for doing joins in later stage. 
    3. Create an output table and ob to read from the staging table, apply business logic transformations and insert the results into the output table in the AWS Glue Data Catalog that users can query using Amazon Athena.
    4. Create an output table and ob to read from the staging table, apply business logic transformations and insert the results into the output table in the AWS Glue Data Catalog that users can query using Amazon Athena.
*/
/* 
Step 0. if you dont see a S3 connection then please use the below SQL to create one

CREATE S3 CONNECTION upsolver_s3_samples
    AWS_ROLE = 'arn:aws:iam::949275490180:role/upsolver_samples_role'
    EXTERNAL_ID = 'SAMPLES'
    READ_ONLY = TRUE;
*/

  
/*
Step 1. Create and ingest into a staging table to store the raw data before transformation. 

    Example code:

    CREATE TABLE default_glue_catalog.<DB_NAME>.orders_raw_data;
*/

CREATE S3 CONNECTION upsolver_s3_samples
    AWS_ROLE = 'arn:aws:iam::949275490180:role/upsolver_samples_role'
    EXTERNAL_ID = 'SAMPLES'
    READ_ONLY = TRUE;


CREATE TABLE default_glue_catalog.<DB_NAME>.event_raw_data()
    partition by $event_date;

CREATE JOB event_staging_job
    CONTENT_TYPE = CSV
    AS COPY FROM S3 upsolver_s3_samples BUCKET = 'upsolver-samples' PREFIX = 'demos/iot/' 
    INTO default_glue_catalog.<DB_NAME>.event_raw_data; 

/* Query the raw data in your staging table, this might take 3-5 minutes for initial load */
select device,att1,att2,dt_updated from default_glue_catalog.<DB_NAME>.event_raw_data;

/*
Step 2. Create a intermediate Materialized view for doing joins in later stage. 

    Example code:

    CREATE MATERIALIZED VIEW default_glue_catalog.<DB_NAME>.abc as select col1,col2,count(*) from table 1 group by col1,col2;
    
*/

CREATE MATERIALIZED VIEW default_glue_catalog.<DB_NAME>.event_rollup_sessions_lookup
AS
SELECT 
device as device,
min(timestamp_cast) AS first_seen_date,
max(timestamp_cast) AS last_seen_date,
DYNAMIC_SESSIONS(unix_timestamp/1000, (15 * 60)) AS sessions_15m
FROM default_glue_catalog.<DB_NAME>.event_raw_data
LET timestamp_cast = PARSE_DATETIME(dt_updated,'yyyy-MM-dd HH:mm'),
unix_timestamp = TO_UNIX_EPOCH_MILLIS(timestamp_cast)
GROUP BY device;

/*      
Step 3. Create a job to read from the staging table, apply business logic transformations and insert the results into the output table.
    Note: It may take 3-4 minutes for the data to appear in your output table.

    Example code:

    CREATE JOB transform_orders_and_insert_into_athena

        START_FROM = BEGINNING
        ADD_MISSING_COLUMNS = true	
        AS INSERT INTO default_glue_catalog.<DB_NAME>.orders_transformed_data MAP_COLUMNS_BY_NAME
        -- Use the SELECT statement to choose columns from the source and implement your business logic transformations.
            SELECT 
                orderid AS order_id, -- rename columns
                MD5(buyeremail) AS customer_id, -- hash or mask columns using built-in functions
                nettotal AS total, 
                $commit_time AS partition_date -- populate the partition column with the processing time of the event, automatically casted to DATE type
            FROM default_glue_catalog.<DB_NAME>.orders_raw_data
            WHERE eventtype = 'ORDER' AND $commit_time BETWEEN run_start_time() AND run_end_time();
*/

CREATE TABLE default_glue_catalog.<DB_NAME>.event_logs_flatten_sessions() COMPUTE_CLUSTER = "Default Compute";

CREATE JOB event_logs_flatten_sessions
    START_FROM = BEGINNING
    ADD_MISSING_COLUMNS = TRUE  
    RUN_INTERVAL = 1 MINUTE
    AS INSERT INTO default_glue_catalog.<DB_NAME>.event_logs_flatten_sessions MAP_COLUMNS_BY_NAME
SELECT
       default_glue_catalog.<DB_NAME>.event_raw_data.device AS device,
       FORMAT_DATETIME(from_unixtime(s.sessions_15m[].startTime), 'yyyy-MM-dd HH:mm:ss') AS s_sessions_15m_starttime,
       FORMAT_DATETIME(from_unixtime(s.sessions_15m[].endTime), 'yyyy-MM-dd HH:mm:ss')  AS s_sessions_15m_endtime,
       (s.sessions_15m[].endTime - s.sessions_15m[].startTime) / 60 AS session_minutes,
       LAST(att1) AS att1,
       LAST(att2) AS att2
FROM default_glue_catalog.<DB_NAME>.event_raw_data 
LEFT JOIN default_glue_catalog.<DB_NAME>.event_rollup_sessions_lookup s ON s.device = default_glue_catalog.<DB_NAME>.event_raw_data.device 
where $commit_time between run_start_time() - PARSE_DURATION('1m')  and run_end_time()
GROUP BY default_glue_catalog.<DB_NAME>.event_raw_data.device,
          FORMAT_DATETIME(from_unixtime(s.sessions_15m[].startTime), 'yyyy-MM-dd HH:mm:ss'),
          FORMAT_DATETIME(from_unixtime(s.sessions_15m[].endTime), 'yyyy-MM-dd HH:mm:ss'),
          (s.sessions_15m[].endTime - s.sessions_15m[].startTime)  / 60;


/* 
    Query the output table to view the results of the transformation job. SQLake queries the table using the Athena APIs.
    Note: It may take 3-4 minutes for the data to appear in your output table.
*/
select device,s_sessions_15m_starttime,s_sessions_15m_endtime,session_minutes,att1,att2 from default_glue_catalog.<DB_NAME>.event_logs_flatten_sessions;

/*      
Step 4. Create a job to read from the staging table, apply business logic transformations and insert the results into the output table.
    Note: It may take 3-4 minutes for the data to appear in your output table.
*/

CREATE TABLE default_glue_catalog.<DB_NAME>.event_logs_device_uptime(
    device string)
PARTITION BY device
PRIMARY KEY device;

CREATE JOB event_logs_device_uptime
    START_FROM = BEGINNING
    ADD_MISSING_COLUMNS = TRUE
    RUN_INTERVAL = 1 MINUTE
AS 
    MERGE INTO default_glue_catalog.<DB_NAME>.event_logs_device_uptime As target
    USING ( 
    SELECT
       default_glue_catalog.<DB_NAME>.event_raw_data.device AS device,
       default_glue_catalog.<DB_NAME>.event_raw_data.att1 AS att1,
       default_glue_catalog.<DB_NAME>.event_raw_data.att2 AS att2,
       s.first_seen_date AS first_seen_date,
       s.last_seen_date AS last_seen_date,
       ((ARRAY_SUM(s.sessions_15m[].endTime - s.sessions_15m[].startTime)/60.0) / DATE_DIFF('minute', s.first_seen_date, s.last_seen_date)) AS uptime
FROM default_glue_catalog.<DB_NAME>.event_raw_data 
LEFT JOIN default_glue_catalog.<DB_NAME>.event_rollup_sessions_lookup s ON s.device = default_glue_catalog.<DB_NAME>.event_raw_data.device 
where $commit_time between run_start_time() and run_end_time()
     
    ) AS source
    ON (source.device = target.device)
    WHEN MATCHED THEN REPLACE
    WHEN NOT MATCHED THEN INSERT MAP_COLUMNS_BY_NAME;

/* 
    Query the output table to view the results of the transformation job. SQLake queries the table using the Athena APIs.
    Note: It may take 3-4 minutes for the data to appear in your output table.
*/        
select device,att1,att2,first_seen_date,last_seen_date,uptime from default_glue_catalog.<DB_NAME>.event_logs_device_uptime;

/*  environment cleanup
*/

drop job event_staging_job;
drop job event_logs_flatten_sessions;
drop job event_logs_device_uptime;

drop MATERIALIZED VIEW default_glue_catalog.<DB_NAME>.event_rollup_sessions_lookup;
drop table default_glue_catalog.<DB_NAME>.event_raw_data delete_data = TRUE;
drop table default_glue_catalog.<DB_NAME>.event_logs_flatten_sessions delete_data = TRUE;
drop table default_glue_catalog.<DB_NAME>.event_logs_device_uptime delete_data = TRUE;

