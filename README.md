Multi-Channel Advertising Analytics Platform
Project Overview
This project integrates advertising data from Facebook Ads, Google Ads, and TikTok into a unified analytics platform using Snowflake. The solution enables cross-platform performance comparison and data-driven budget optimization.
Business Problem
Marketing teams running campaigns across multiple advertising platforms face several challenges:

Each platform uses different column names and data formats
Metrics are calculated differently across platforms
No centralized view for cross-platform comparison
Manual data consolidation is time-consuming and error-prone

Solution
Built a data integration pipeline that:

Normalizes data from three advertising platforms
Standardizes metric calculations (CTR, CPC, Cost per Conversion, Conversion Rate)
Creates a unified data model for analysis
Provides ready-to-use SQL queries for insights

Technical Architecture
Data Sources

Facebook Ads (120 daily records)
Google Ads (120 daily records)
TikTok Ads (120 daily records)

Data Warehouse

Snowflake Cloud Data Platform

Data Pipeline

Raw data ingestion into staging tables
Data normalization using SQL transformations
Unified table creation with UNION ALL
Analytical views for dashboard consumption

Database Schema
Staging Tables
facebook_ads

date, campaign_id, campaign_name, ad_set_id, ad_set_name
impressions, clicks, spend, conversions
video_views, engagement_rate, reach, frequency

google_ads

date, campaign_id, campaign_name, ad_group_id, ad_group_name
impressions, clicks, cost, conversions, conversion_value
ctr, avg_cpc, quality_score, search_impression_share

tiktok_ads

date, campaign_id, campaign_name, adgroup_id, adgroup_name
impressions, clicks, cost, conversions
video_views, video_watch_25, video_watch_50, video_watch_75, video_watch_100
likes, shares, comments

Unified Table
unified_advertising_data

date, platform, campaign_id, campaign_name
impressions, clicks, cost, conversions
video_views, reach
ctr, cpc, cost_per_conversion, conversion_rate, engagement_rate_pct

Key Transformations
Data Normalization

Standardized cost column (Facebook "spend" to "cost")
Calculated consistent metrics across all platforms
Handled platform-specific fields with NULL values
Technologies Used

Snowflake (Cloud Data Warehouse)
SQL (Data Transformation)
CSV (Data Format)
