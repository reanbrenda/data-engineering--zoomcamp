-- =====================================================
-- NETWORK RAIL MOVEMENT ANALYTICS - SQL QUERIES
-- Author: Brenda
-- Project: raildata-469117.networkrail.movements
-- =====================================================

-- =====================================================
-- 1. PUNCTUALITY & PERFORMANCE CHARTS
-- =====================================================

-- Punctuality Distribution (Donut Chart)
SELECT 
    variation_status,
    COUNT(*) as movement_count
FROM `raildata-469117.networkrail.movements`
GROUP BY variation_status
ORDER BY movement_count DESC;

-- TOC Performance Ranking (Horizontal Bar Chart)
SELECT 
    toc_id,
    COUNT(*) as total_movements,
    COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) as on_time_count,
    ROUND(COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*), 2) as punctuality_percentage
FROM `raildata-469117.networkrail.movements`
GROUP BY toc_id
HAVING total_movements > 10
ORDER BY punctuality_percentage DESC
LIMIT 20;

-- Punctuality by TOC Heatmap
SELECT 
    toc_id,
    variation_status,
    COUNT(*) as movement_count
FROM `raildata-469117.networkrail.movements`
GROUP BY toc_id, variation_status
ORDER BY toc_id, variation_status;

-- =====================================================
-- 2. TIME-BASED ANALYSIS CHARTS
-- =====================================================

-- Hourly Movement Patterns (Line Chart with Area)
SELECT 
    EXTRACT(HOUR FROM actual_timestamp) as hour_of_day,
    COUNT(*) as total_movements,
    COUNT(CASE WHEN event_type = 'DEPARTURE' THEN 1 END) as departures,
    COUNT(CASE WHEN event_type = 'ARRIVAL' THEN 1 END) as arrivals
FROM `raildata-469117.networkrail.movements`
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- Daily Movement Trends (Multi-line Chart)
SELECT 
    DATE(actual_timestamp) as date,
    variation_status,
    COUNT(*) as movement_count
FROM `raildata-469117.networkrail.movements`
GROUP BY date, variation_status
ORDER BY date DESC
LIMIT 30;

-- Weekly Performance Calendar (Calendar Heatmap)
SELECT 
    EXTRACT(DAYOFWEEK FROM actual_timestamp) as day_of_week,
    EXTRACT(HOUR FROM actual_timestamp) as hour_of_day,
    ROUND(COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*), 2) as punctuality_percentage
FROM `raildata-469117.networkrail.movements`
GROUP BY day_of_week, hour_of_day
ORDER BY day_of_week, hour_of_day;

-- =====================================================
-- 3. GEOGRAPHIC & STATION CHARTS
-- =====================================================

-- Top Stations by Activity (Horizontal Bar Chart)
SELECT 
    loc_stanox as station_code,
    COUNT(*) as movement_count,
    COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) as on_time_count,
    ROUND(COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*), 2) as punctuality_percentage
FROM `raildata-469117.networkrail.movements`
WHERE loc_stanox IS NOT NULL
GROUP BY loc_stanox
HAVING movement_count > 5
ORDER BY movement_count DESC
LIMIT 20;

-- Route Performance Analysis (Sankey Diagram)
SELECT 
    toc_id,
    route,
    variation_status,
    COUNT(*) as movement_count
FROM `raildata-469117.networkrail.movements`
WHERE route IS NOT NULL AND route != '0'
GROUP BY toc_id, route, variation_status
ORDER BY toc_id, route;

-- =====================================================
-- 4. ADVANCED ANALYTICS CHARTS
-- =====================================================

-- Delay Distribution Analysis (Histogram)
SELECT 
    ROUND((actual_timestamp - planned_timestamp) / 60, 0) as delay_minutes,
    COUNT(*) as frequency
FROM `raildata-469117.networkrail.movements`
WHERE variation_status = 'LATE' 
  AND actual_timestamp IS NOT NULL 
  AND planned_timestamp IS NOT NULL
GROUP BY delay_minutes
ORDER BY delay_minutes;

-- Performance Correlation Matrix (Heatmap)
SELECT 
    toc_id,
    EXTRACT(HOUR FROM actual_timestamp) as hour_of_day,
    ROUND(COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*), 2) as punctuality_percentage
FROM `raildata-469117.networkrail.movements`
GROUP BY toc_id, hour_of_day
HAVING COUNT(*) > 5
ORDER BY toc_id, hour_of_day;

-- =====================================================
-- 5. INTERACTIVE DASHBOARD ELEMENTS
-- =====================================================

-- Real-Time Performance Gauge
SELECT 
    ROUND(COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*), 2) as current_punctuality_percentage
FROM `raildata-469117.networkrail.movements`
WHERE DATE(actual_timestamp) = CURRENT_DATE();

-- Performance Leaderboard (Animated Bar Chart)
SELECT 
    toc_id,
    COUNT(*) as total_movements,
    ROUND(COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*), 2) as punctuality_percentage
FROM `raildata-469117.networkrail.movements`
GROUP BY toc_id
HAVING total_movements > 20
ORDER BY punctuality_percentage DESC
LIMIT 15;

-- =====================================================
-- 6. EXECUTIVE SUMMARY CHARTS
-- =====================================================

-- KPI Scorecard
SELECT 
    COUNT(*) as total_movements,
    COUNT(DISTINCT train_id) as unique_trains,
    COUNT(DISTINCT toc_id) as active_tocs,
    ROUND(COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*), 2) as overall_punctuality_percentage
FROM `raildata-469117.networkrail.movements`;

-- Network Health Index (Radar Chart)
SELECT 
    ROUND(COUNT(CASE WHEN variation_status = 'ON TIME' THEN 1 END) * 100.0 / COUNT(*), 2) as punctuality_score,
    ROUND(COUNT(DISTINCT train_id) * 100.0 / COUNT(*), 2) as coverage_score,
    ROUND(COUNT(CASE WHEN variation_status != 'LATE' THEN 1 END) * 100.0 / COUNT(*), 2) as reliability_score,
    ROUND(COUNT(DISTINCT toc_id) * 100.0 / 50, 2) as diversity_score
FROM `raildata-469117.networkrail.movements`;

-- =====================================================
-- 7. CREATIVE & FUN CHARTS
-- =====================================================

-- Train Journey Visualization (Flow Diagram)
SELECT 
    event_type,
    COUNT(*) as movement_count,
    ROUND(AVG(CASE WHEN variation_status = 'LATE' THEN (actual_timestamp - planned_timestamp) / 60 ELSE 0 END), 2) as avg_delay_minutes
FROM `raildata-469117.networkrail.movements`
GROUP BY event_type;

-- =====================================================
-- QUICK TEST QUERIES
-- =====================================================

-- Test 1: Basic Count
SELECT COUNT(*) as total_rows FROM `raildata-469117.networkrail.movements`;

-- Test 2: Sample Data
SELECT * FROM `raildata-469117.networkrail.movements` LIMIT 5;

-- Test 3: Column Check
SELECT 
    column_name,
    data_type
FROM `raildata-469117.networkrail.movements.INFORMATION_SCHEMA.COLUMNS`
ORDER BY ordinal_position;

-- =====================================================
-- USAGE INSTRUCTIONS
-- =====================================================

/*
WHERE TO USE THESE QUERIES:

1. BIGQUERY CONSOLE (Testing & Development):
   - Go to: https://console.cloud.google.com/bigquery
   - Select project: raildata-469117
   - Click "QUERY" button
   - Copy-paste any query above
   - Click "RUN"

2. LOOKER STUDIO (Dashboard Building):
   - Go to: https://lookerstudio.google.com/
   - Create new data source
   - Select BigQuery connector
   - Choose "Custom Query" option
   - Copy-paste any query above
   - Use results to create charts

3. PYTHON SCRIPT (Automated Analysis):
   - Use: playground/bigquery_queries.py
   - Run: python bigquery_queries.py
   - Choose from menu options

4. SCHEDULED REPORTS:
   - Set up BigQuery scheduled queries
   - Export results to Google Sheets
   - Create automated dashboards

TIPS:
- Always test queries in BigQuery Console first
- Use LIMIT clauses when testing large datasets
- Save successful queries for reuse
- Monitor query costs and performance
*/
