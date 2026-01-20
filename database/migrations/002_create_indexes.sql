-- Database Migration: Create Indexes
-- Commerce Analytics Platform

-- Platform metrics indexes
CREATE INDEX IF NOT EXISTS idx_platform_metrics_window ON platform_metrics (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_platform_metrics_platform ON platform_metrics (platform);
CREATE INDEX IF NOT EXISTS idx_platform_metrics_platform_window ON platform_metrics (platform, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_platform_metrics_created_at ON platform_metrics (created_at DESC);

-- Brand metrics indexes
CREATE INDEX IF NOT EXISTS idx_brand_metrics_window ON brand_metrics (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_brand_metrics_brand ON brand_metrics (brand);
CREATE INDEX IF NOT EXISTS idx_brand_metrics_brand_window ON brand_metrics (brand, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_brand_metrics_created_at ON brand_metrics (created_at DESC);

-- Trending content indexes
CREATE INDEX IF NOT EXISTS idx_trending_content_window ON trending_content (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_trending_content_platform ON trending_content (platform);
CREATE INDEX IF NOT EXISTS idx_trending_content_platform_rank ON trending_content (platform, rank);
CREATE INDEX IF NOT EXISTS idx_trending_content_content_id ON trending_content (content_id);
CREATE INDEX IF NOT EXISTS idx_trending_content_buzz_score ON trending_content (buzz_score DESC);
CREATE INDEX IF NOT EXISTS idx_trending_content_detected_at ON trending_content (detected_at DESC);

-- Sentiment analysis indexes
CREATE INDEX IF NOT EXISTS idx_sentiment_content_id ON sentiment_analysis (content_id);
CREATE INDEX IF NOT EXISTS idx_sentiment_platform ON sentiment_analysis (platform);
CREATE INDEX IF NOT EXISTS idx_sentiment_sentiment ON sentiment_analysis (sentiment);
CREATE INDEX IF NOT EXISTS idx_sentiment_platform_sentiment ON sentiment_analysis (platform, sentiment);
CREATE INDEX IF NOT EXISTS idx_sentiment_processed_at ON sentiment_analysis (processed_at DESC);

-- Full-text search index for sentiment text
CREATE INDEX IF NOT EXISTS idx_sentiment_text_gin ON sentiment_analysis USING gin(to_tsvector('simple', text));

-- Cross-platform analysis indexes
CREATE INDEX IF NOT EXISTS idx_cross_platform_window ON cross_platform_analysis (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_cross_platform_brand ON cross_platform_analysis (brand);
CREATE INDEX IF NOT EXISTS idx_cross_platform_brand_window ON cross_platform_analysis (brand, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_cross_platform_score ON cross_platform_analysis (cross_platform_score DESC);

-- Keyword trends indexes
CREATE INDEX IF NOT EXISTS idx_keyword_trends_window ON keyword_trends (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_keyword_trends_platform ON keyword_trends (platform);
CREATE INDEX IF NOT EXISTS idx_keyword_trends_keyword ON keyword_trends (keyword);
CREATE INDEX IF NOT EXISTS idx_keyword_trends_platform_rank ON keyword_trends (platform, rank);
CREATE INDEX IF NOT EXISTS idx_keyword_trends_count ON keyword_trends (count DESC);

-- Hashtag trends indexes
CREATE INDEX IF NOT EXISTS idx_hashtag_trends_window ON hashtag_trends (window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_hashtag_trends_platform ON hashtag_trends (platform);
CREATE INDEX IF NOT EXISTS idx_hashtag_trends_hashtag ON hashtag_trends (hashtag);
CREATE INDEX IF NOT EXISTS idx_hashtag_trends_platform_rank ON hashtag_trends (platform, rank);

-- Alert history indexes
CREATE INDEX IF NOT EXISTS idx_alert_history_type ON alert_history (alert_type);
CREATE INDEX IF NOT EXISTS idx_alert_history_platform ON alert_history (platform);
CREATE INDEX IF NOT EXISTS idx_alert_history_sent_at ON alert_history (sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_history_content_id ON alert_history (content_id);

-- API quota indexes
CREATE INDEX IF NOT EXISTS idx_api_quota_platform ON api_quota_usage (platform);
CREATE INDEX IF NOT EXISTS idx_api_quota_date ON api_quota_usage (date DESC);

-- Content metadata indexes
CREATE INDEX IF NOT EXISTS idx_content_metadata_platform ON content_metadata (platform);
CREATE INDEX IF NOT EXISTS idx_content_metadata_content_type ON content_metadata (content_type);
CREATE INDEX IF NOT EXISTS idx_content_metadata_author_id ON content_metadata (author_id);
CREATE INDEX IF NOT EXISTS idx_content_metadata_published_at ON content_metadata (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_content_metadata_last_updated ON content_metadata (last_updated_at DESC);

-- Full-text search indexes for content
CREATE INDEX IF NOT EXISTS idx_content_title_gin ON content_metadata USING gin(to_tsvector('simple', title));
CREATE INDEX IF NOT EXISTS idx_content_description_gin ON content_metadata USING gin(to_tsvector('simple', description));

-- Daily summary indexes
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_summary (date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_summary_platform ON daily_summary (platform);
CREATE INDEX IF NOT EXISTS idx_daily_summary_date_platform ON daily_summary (date DESC, platform);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_platform_metrics_query ON platform_metrics (platform, window_start DESC, total_views DESC);
CREATE INDEX IF NOT EXISTS idx_trending_query ON trending_content (platform, window_start DESC, buzz_score DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_query ON sentiment_analysis (platform, sentiment, processed_at DESC);
