create warehouse Adverstising;
use warehouse Adverstising;
-- Step 1: Create Database and Schema
-- =====================================================
CREATE DATABASE IF NOT EXISTS ADVERTISING_DATA;
CREATE SCHEMA IF NOT EXISTS ADVERTISING_DATA.MULTI_CHANNEL;

USE DATABASE ADVERTISING_DATA;
USE SCHEMA MULTI_CHANNEL;

CREATE OR REPLACE TABLE facebook_ads (
    date DATE,
    campaign_id VARCHAR(50),
    campaign_name VARCHAR(255),
    ad_set_id VARCHAR(50), 
    ad_set_name VARCHAR(255),
    impressions NUMBER,
    clicks NUMBER,
    spend DECIMAL(10,2),
    conversions NUMBER,
    video_views NUMBER,
    engagement_rate DECIMAL(6,4),
    reach NUMBER,
    frequency DECIMAL(4,2)
);
-- Google Ads Table
CREATE OR REPLACE TABLE google_ads (
    date DATE,
    campaign_id VARCHAR(50),
    campaign_name VARCHAR(255),
    ad_group_id VARCHAR(50),
    ad_group_name VARCHAR(255),
    impressions NUMBER,
    clicks NUMBER,
    cost DECIMAL(10,2),
    conversions NUMBER,
    conversion_value DECIMAL(10,2),
    ctr DECIMAL(6,4),
    avg_cpc DECIMAL(6,2),
    quality_score NUMBER,
    search_impression_share DECIMAL(4,2)
);
-- TikTok Ads Table
CREATE OR REPLACE TABLE tiktok_ads (
    date DATE,
    campaign_id VARCHAR(50),
    campaign_name VARCHAR(255),
    adgroup_id VARCHAR(50),
    adgroup_name VARCHAR(255),
    impressions NUMBER,
    clicks NUMBER,
    cost DECIMAL(10,2),
    conversions NUMBER,
    video_views NUMBER,
    video_watch_25 NUMBER,
    video_watch_50 NUMBER,
    video_watch_75 NUMBER,
    video_watch_100 NUMBER,
    likes NUMBER,
    shares NUMBER,
    comments NUMBER
);
-- Step 3: Create File Format and Stage for CSV Upload
-- =====================================================
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = '\t'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    DATE_FORMAT = 'DD/MM/YYYY'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE STAGE advertising_stage
    FILE_FORMAT = csv_format;
-- Step 4: Load Data (Execute after uploading CSVs to stage)
-- =====================================================
-- Upload your CSV files through Snowflake UI to the stage, then run:

COPY INTO facebook_ads
FROM @advertising_stage/facebook_ads.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO google_ads
FROM @advertising_stage/google_ads.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO tiktok_ads
FROM @advertising_stage/tiktok_ads.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Step 5: Verify Data Load
-- =====================================================
SELECT 'Facebook' AS platform, COUNT(*) AS row_count FROM facebook_ads
UNION ALL
SELECT 'Google' AS platform, COUNT(*) AS row_count FROM google_ads
UNION ALL
SELECT 'TikTok' AS platform, COUNT(*) AS row_count FROM tiktok_ads;

CREATE OR REPLACE TABLE unified_advertising_data AS
WITH facebook_normalized AS (
    SELECT  
        date,
        campaign_id,
        campaign_name,
        'Facebook' AS platform,
        impressions,
        clicks,
        spend AS cost,
        conversions,
        video_views,
        reach,
        CASE WHEN impressions > 0 THEN (clicks::DECIMAL / impressions) * 100 ELSE 0 END AS ctr,
        CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END AS cpc,
        CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cost_per_conversion,
        CASE WHEN clicks > 0 THEN (conversions::DECIMAL / clicks) * 100 ELSE 0 END AS conversion_rate,
        engagement_rate * 100 AS engagement_rate_pct
    FROM facebook_ads
),
google_normalized AS (
    SELECT 
        date,
        campaign_id,
        campaign_name,
        'Google' AS platform,
        impressions,
        clicks,
        cost,
        conversions,
        NULL AS video_views,
        NULL AS reach,
        ctr * 100 AS ctr,
        avg_cpc AS cpc,
        CASE WHEN conversions > 0 THEN cost / conversions ELSE 0 END AS cost_per_conversion,
        CASE WHEN clicks > 0 THEN (conversions::DECIMAL / clicks) * 100 ELSE 0 END AS conversion_rate,
        NULL AS engagement_rate_pct
    FROM google_ads
),
tiktok_normalized AS (
    SELECT 
        date,
        campaign_id,
        campaign_name,
        'TikTok' AS platform,
        impressions,
        clicks,
        cost,
        conversions,
        video_views,
        NULL AS reach,
        CASE WHEN impressions > 0 THEN (clicks::DECIMAL / impressions) * 100 ELSE 0 END AS ctr,
        CASE WHEN clicks > 0 THEN cost / clicks ELSE 0 END AS cpc,
        CASE WHEN conversions > 0 THEN cost / conversions ELSE 0 END AS cost_per_conversion,
        CASE WHEN clicks > 0 THEN (conversions::DECIMAL / clicks) * 100 ELSE 0 END AS conversion_rate,
        CASE WHEN impressions > 0 THEN ((likes + shares + comments)::DECIMAL / impressions) * 100 ELSE 0 END AS engagement_rate_pct
    FROM tiktok_ads
)SELECT * FROM facebook_normalized
UNION ALL
SELECT * FROM google_normalized
UNION ALL
SELECT * FROM tiktok_normalized;

select * from unified_advertising_data;

-- Verify unified table
SELECT platform, COUNT(*) AS row_count 
FROM unified_advertising_data 
GROUP BY platform;

select * from unified_advertising_data;
-- Step 7: Dashboard Queries
-- =====================================================

-- Query 1: Platform Performance Summary
SELECT
    platform,
    COUNT(DISTINCT campaign_name) AS total_campaigns,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(cost) AS total_spend,
    SUM(conversions) AS total_conversions,
    ROUND(AVG(ctr), 2) AS avg_ctr,
    ROUND(AVG(cpc), 2) AS avg_cpc,
    ROUND(SUM(cost) / NULLIF(SUM(conversions), 0), 2) AS overall_cost_per_conversion,
    ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate
FROM unified_advertising_data
GROUP BY platform
ORDER BY total_spend DESC;

-- Query 2: Daily Performance Trend
SELECT
    date,
    platform,
    SUM(cost) AS daily_spend,
    SUM(clicks) AS daily_clicks,
    SUM(conversions) AS daily_conversions,
    ROUND(AVG(ctr), 2) AS avg_ctr
FROM unified_advertising_data
GROUP BY date, platform
ORDER BY date, platform;

-- Query 3: Top 10 Campaigns by Conversions
SELECT
    campaign_name,
    platform,
    SUM(cost) AS total_spend,
    SUM(conversions) AS total_conversions,
    ROUND(SUM(cost) / NULLIF(SUM(conversions), 0), 2) AS cost_per_conversion,
    ROUND(AVG(ctr), 2) AS avg_ctr,
    ROUND(AVG(conversion_rate), 2) AS avg_conversion_rate
FROM unified_advertising_data
WHERE conversions > 0
GROUP BY campaign_name, platform
ORDER BY total_conversions DESC
LIMIT 10;

-- Query 4: Overall Performance Metrics (KPIs)
SELECT
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(cost) AS total_spend,
    SUM(conversions) AS total_conversions,
    ROUND((SUM(clicks)::DECIMAL / NULLIF(SUM(impressions), 0)) * 100, 2) AS overall_ctr,
    ROUND(SUM(cost) / NULLIF(SUM(clicks), 0), 2) AS overall_cpc,
    ROUND(SUM(cost) / NULLIF(SUM(conversions), 0), 2) AS overall_cost_per_conversion,
    ROUND((SUM(conversions)::DECIMAL / NULLIF(SUM(clicks), 0)) * 100, 2) AS overall_conversion_rate
FROM unified_advertising_data;

-- Query 5: Platform Budget Share
SELECT
    platform,
    SUM(cost) AS total_spend,
    ROUND((SUM(cost) / (SELECT SUM(cost) FROM unified_advertising_data)) * 100, 2) AS budget_share_pct
FROM unified_advertising_data
GROUP BY platform
ORDER BY total_spend DESC;

