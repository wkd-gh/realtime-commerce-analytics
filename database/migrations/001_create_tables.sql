-- Database Migration: Create Tables
-- Commerce Analytics Platform

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Platform metrics table (5-minute aggregations)
CREATE TABLE IF NOT EXISTS platform_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    platform VARCHAR(50) NOT NULL,
    total_content BIGINT DEFAULT 0,
    total_views BIGINT DEFAULT 0,
    total_likes BIGINT DEFAULT 0,
    total_comments BIGINT DEFAULT 0,
    total_shares BIGINT DEFAULT 0,
    avg_engagement_rate DECIMAL(10, 4) DEFAULT 0,
    max_views BIGINT DEFAULT 0,
    unique_authors BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Brand metrics table
CREATE TABLE IF NOT EXISTS brand_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    brand VARCHAR(100) NOT NULL,
    total_mentions BIGINT DEFAULT 0,
    total_reach BIGINT DEFAULT 0,
    total_likes BIGINT DEFAULT 0,
    avg_engagement DECIMAL(10, 4) DEFAULT 0,
    platform_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trending content table
CREATE TABLE IF NOT EXISTS trending_content (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    platform VARCHAR(50) NOT NULL,
    rank INTEGER NOT NULL,
    content_id VARCHAR(255) NOT NULL,
    title TEXT,
    author_name VARCHAR(255),
    total_views BIGINT DEFAULT 0,
    total_likes BIGINT DEFAULT 0,
    engagement_rate DECIMAL(10, 4) DEFAULT 0,
    buzz_score DECIMAL(10, 4) DEFAULT 0,
    z_score DECIMAL(10, 4) DEFAULT 0,
    url TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sentiment analysis results
CREATE TABLE IF NOT EXISTS sentiment_analysis (
    id SERIAL PRIMARY KEY,
    content_id VARCHAR(255) NOT NULL,
    platform VARCHAR(50) NOT NULL,
    text TEXT,
    sentiment VARCHAR(20) NOT NULL,
    sentiment_score DECIMAL(5, 4) DEFAULT 0.5,
    confidence DECIMAL(5, 4) DEFAULT 0.5,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cross-platform analysis
CREATE TABLE IF NOT EXISTS cross_platform_analysis (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    brand VARCHAR(100) NOT NULL,
    youtube_views BIGINT DEFAULT 0,
    youtube_engagement DECIMAL(10, 4) DEFAULT 0,
    twitter_mentions BIGINT DEFAULT 0,
    twitter_sentiment DECIMAL(5, 4) DEFAULT 0.5,
    tiktok_views BIGINT DEFAULT 0,
    tiktok_engagement DECIMAL(10, 4) DEFAULT 0,
    naver_search_count BIGINT DEFAULT 0,
    naver_avg_price DECIMAL(15, 2) DEFAULT 0,
    google_trends_score DECIMAL(10, 4) DEFAULT 0,
    total_reach BIGINT DEFAULT 0,
    cross_platform_score DECIMAL(10, 4) DEFAULT 0,
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Keyword trends
CREATE TABLE IF NOT EXISTS keyword_trends (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    platform VARCHAR(50) NOT NULL,
    rank INTEGER NOT NULL,
    keyword VARCHAR(255) NOT NULL,
    count BIGINT DEFAULT 0,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Hashtag trends
CREATE TABLE IF NOT EXISTS hashtag_trends (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    platform VARCHAR(50) NOT NULL,
    rank INTEGER NOT NULL,
    hashtag VARCHAR(255) NOT NULL,
    count BIGINT DEFAULT 0,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alert history
CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    platform VARCHAR(50),
    content_id VARCHAR(255),
    title TEXT,
    metric_name VARCHAR(100),
    current_value DECIMAL(15, 4),
    expected_value DECIMAL(15, 4),
    z_score DECIMAL(10, 4),
    message TEXT,
    sent_to VARCHAR(50) DEFAULT 'slack',
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- API quota tracking
CREATE TABLE IF NOT EXISTS api_quota_usage (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    requests_made BIGINT DEFAULT 0,
    quota_limit BIGINT NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, date)
);

-- Content metadata (for reference)
CREATE TABLE IF NOT EXISTS content_metadata (
    id SERIAL PRIMARY KEY,
    content_id VARCHAR(255) NOT NULL UNIQUE,
    platform VARCHAR(50) NOT NULL,
    content_type VARCHAR(50) NOT NULL,
    title TEXT,
    description TEXT,
    author_id VARCHAR(255),
    author_name VARCHAR(255),
    url TEXT,
    thumbnail_url TEXT,
    published_at TIMESTAMP,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily summary stats
CREATE TABLE IF NOT EXISTS daily_summary (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    platform VARCHAR(50) NOT NULL,
    total_content BIGINT DEFAULT 0,
    total_views BIGINT DEFAULT 0,
    total_likes BIGINT DEFAULT 0,
    total_comments BIGINT DEFAULT 0,
    avg_engagement_rate DECIMAL(10, 4) DEFAULT 0,
    trending_count INTEGER DEFAULT 0,
    top_content_id VARCHAR(255),
    top_content_views BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, platform)
);

-- Comments for tables
COMMENT ON TABLE platform_metrics IS 'Aggregated metrics by platform in 5-minute windows';
COMMENT ON TABLE brand_metrics IS 'Aggregated metrics by brand across platforms';
COMMENT ON TABLE trending_content IS 'Top trending content detected by the system';
COMMENT ON TABLE sentiment_analysis IS 'Sentiment analysis results for content';
COMMENT ON TABLE cross_platform_analysis IS 'Cross-platform brand analysis results';
COMMENT ON TABLE keyword_trends IS 'Trending keywords extracted from content';
COMMENT ON TABLE alert_history IS 'History of alerts sent via Slack';
COMMENT ON TABLE api_quota_usage IS 'API quota tracking for rate limiting';
