-- ClickHouse Database Initialization
-- Commerce Analytics Platform - Time Series Data

-- Create database
CREATE DATABASE IF NOT EXISTS commerce_analytics;

USE commerce_analytics;

-- Raw events table (high-volume time series)
CREATE TABLE IF NOT EXISTS raw_events
(
    event_time DateTime64(3),
    message_id String,
    platform LowCardinality(String),
    content_type LowCardinality(String),
    content_id String,
    title String,
    text String,
    author_id String,
    author_name String,
    views UInt64,
    likes UInt64,
    comments UInt64,
    shares UInt64,
    engagement_rate Float64,
    sentiment LowCardinality(String),
    sentiment_score Float64,
    brand Nullable(String),
    category Nullable(String),
    keywords Array(String),
    hashtags Array(String),
    url String,
    processing_time DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (platform, event_time, content_id)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Metrics time series (aggregated)
CREATE TABLE IF NOT EXISTS metrics_timeseries
(
    timestamp DateTime,
    platform LowCardinality(String),
    metric_name LowCardinality(String),
    metric_value Float64,
    dimensions Map(String, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (platform, metric_name, timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Platform metrics (5-minute rollup)
CREATE TABLE IF NOT EXISTS platform_metrics_5m
(
    window_start DateTime,
    window_end DateTime,
    platform LowCardinality(String),
    total_content UInt64,
    total_views UInt64,
    total_likes UInt64,
    total_comments UInt64,
    total_shares UInt64,
    avg_engagement_rate Float64,
    max_views UInt64,
    unique_authors UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (platform, window_start)
TTL window_start + INTERVAL 90 DAY;

-- Platform metrics (hourly rollup)
CREATE TABLE IF NOT EXISTS platform_metrics_1h
(
    window_start DateTime,
    window_end DateTime,
    platform LowCardinality(String),
    total_content UInt64,
    total_views UInt64,
    total_likes UInt64,
    total_comments UInt64,
    total_shares UInt64,
    avg_engagement_rate Float64,
    max_views UInt64,
    unique_authors UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (platform, window_start)
TTL window_start + INTERVAL 365 DAY;

-- Content performance tracking
CREATE TABLE IF NOT EXISTS content_performance
(
    measured_at DateTime,
    content_id String,
    platform LowCardinality(String),
    views UInt64,
    likes UInt64,
    comments UInt64,
    shares UInt64,
    engagement_rate Float64,
    views_delta Int64,
    likes_delta Int64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(measured_at)
ORDER BY (platform, content_id, measured_at)
TTL measured_at + INTERVAL 30 DAY;

-- Brand mentions tracking
CREATE TABLE IF NOT EXISTS brand_mentions
(
    event_time DateTime,
    brand String,
    platform LowCardinality(String),
    content_id String,
    sentiment LowCardinality(String),
    sentiment_score Float64,
    views UInt64,
    engagement_rate Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (brand, platform, event_time)
TTL event_time + INTERVAL 90 DAY;

-- Keyword frequency tracking
CREATE TABLE IF NOT EXISTS keyword_frequency
(
    window_start DateTime,
    platform LowCardinality(String),
    keyword String,
    frequency UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (platform, keyword, window_start)
TTL window_start + INTERVAL 30 DAY;

-- Hashtag frequency tracking
CREATE TABLE IF NOT EXISTS hashtag_frequency
(
    window_start DateTime,
    platform LowCardinality(String),
    hashtag String,
    frequency UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (platform, hashtag, window_start)
TTL window_start + INTERVAL 30 DAY;

-- Trending content history
CREATE TABLE IF NOT EXISTS trending_history
(
    detected_at DateTime,
    platform LowCardinality(String),
    content_id String,
    title String,
    views UInt64,
    buzz_score Float64,
    z_score Float64,
    trend_level LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (platform, detected_at, buzz_score DESC)
TTL detected_at + INTERVAL 90 DAY;

-- Materialized view for hourly aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_metrics
TO platform_metrics_1h
AS SELECT
    toStartOfHour(window_start) AS window_start,
    toStartOfHour(window_start) + INTERVAL 1 HOUR AS window_end,
    platform,
    sum(total_content) AS total_content,
    sum(total_views) AS total_views,
    sum(total_likes) AS total_likes,
    sum(total_comments) AS total_comments,
    sum(total_shares) AS total_shares,
    avg(avg_engagement_rate) AS avg_engagement_rate,
    max(max_views) AS max_views,
    max(unique_authors) AS unique_authors
FROM platform_metrics_5m
GROUP BY toStartOfHour(window_start), platform;

-- Materialized view for keyword aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_keyword_hourly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (platform, keyword, hour)
AS SELECT
    toStartOfHour(window_start) AS hour,
    platform,
    keyword,
    sum(frequency) AS total_frequency
FROM keyword_frequency
GROUP BY toStartOfHour(window_start), platform, keyword;

-- Views for analytics

-- Real-time metrics (last hour)
CREATE VIEW IF NOT EXISTS v_realtime_metrics AS
SELECT
    platform,
    sum(total_views) AS views_1h,
    sum(total_likes) AS likes_1h,
    sum(total_comments) AS comments_1h,
    avg(avg_engagement_rate) AS avg_engagement_1h,
    sum(total_content) AS content_count_1h
FROM platform_metrics_5m
WHERE window_start >= now() - INTERVAL 1 HOUR
GROUP BY platform;

-- Daily platform comparison
CREATE VIEW IF NOT EXISTS v_daily_platform_stats AS
SELECT
    toDate(window_start) AS date,
    platform,
    sum(total_views) AS daily_views,
    sum(total_likes) AS daily_likes,
    sum(total_content) AS daily_content,
    avg(avg_engagement_rate) AS daily_avg_engagement
FROM platform_metrics_5m
WHERE window_start >= today() - 7
GROUP BY toDate(window_start), platform
ORDER BY date DESC, platform;

-- Top keywords (last 6 hours)
CREATE VIEW IF NOT EXISTS v_top_keywords AS
SELECT
    platform,
    keyword,
    sum(frequency) AS total_frequency
FROM keyword_frequency
WHERE window_start >= now() - INTERVAL 6 HOUR
GROUP BY platform, keyword
ORDER BY total_frequency DESC
LIMIT 100;

-- Brand performance over time
CREATE VIEW IF NOT EXISTS v_brand_timeline AS
SELECT
    toStartOfHour(event_time) AS hour,
    brand,
    count() AS mention_count,
    sum(views) AS total_views,
    avg(sentiment_score) AS avg_sentiment,
    countIf(sentiment = 'positive') AS positive_count,
    countIf(sentiment = 'negative') AS negative_count
FROM brand_mentions
WHERE event_time >= now() - INTERVAL 24 HOUR
GROUP BY toStartOfHour(event_time), brand
ORDER BY hour DESC, mention_count DESC;
