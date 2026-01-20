-- Database Migration: Create Views
-- Commerce Analytics Platform

-- Latest platform metrics view
CREATE OR REPLACE VIEW v_latest_platform_metrics AS
SELECT DISTINCT ON (platform)
    platform,
    window_start,
    window_end,
    total_content,
    total_views,
    total_likes,
    total_comments,
    total_shares,
    avg_engagement_rate,
    max_views,
    unique_authors,
    created_at
FROM platform_metrics
ORDER BY platform, window_start DESC;

-- Hourly platform summary
CREATE OR REPLACE VIEW v_hourly_platform_summary AS
SELECT
    date_trunc('hour', window_start) AS hour,
    platform,
    SUM(total_content) AS total_content,
    SUM(total_views) AS total_views,
    SUM(total_likes) AS total_likes,
    SUM(total_comments) AS total_comments,
    AVG(avg_engagement_rate) AS avg_engagement_rate,
    MAX(max_views) AS max_views,
    COUNT(DISTINCT unique_authors) AS unique_authors
FROM platform_metrics
WHERE window_start >= NOW() - INTERVAL '24 hours'
GROUP BY date_trunc('hour', window_start), platform
ORDER BY hour DESC, platform;

-- Top trending content (last hour)
CREATE OR REPLACE VIEW v_top_trending_now AS
SELECT
    t.platform,
    t.rank,
    t.content_id,
    t.title,
    t.total_views,
    t.total_likes,
    t.engagement_rate,
    t.buzz_score,
    t.z_score,
    t.url,
    t.detected_at
FROM trending_content t
WHERE t.window_start >= NOW() - INTERVAL '1 hour'
  AND t.rank <= 10
ORDER BY t.buzz_score DESC;

-- Brand performance comparison
CREATE OR REPLACE VIEW v_brand_performance AS
SELECT
    b.brand,
    b.window_start,
    b.total_mentions,
    b.total_reach,
    b.avg_engagement,
    cp.youtube_views,
    cp.youtube_engagement,
    cp.twitter_mentions,
    cp.tiktok_views,
    cp.tiktok_engagement,
    cp.cross_platform_score
FROM brand_metrics b
LEFT JOIN cross_platform_analysis cp
    ON b.brand = cp.brand
    AND b.window_start = cp.window_start
WHERE b.window_start >= NOW() - INTERVAL '24 hours'
ORDER BY b.window_start DESC, cp.cross_platform_score DESC NULLS LAST;

-- Sentiment distribution by platform
CREATE OR REPLACE VIEW v_sentiment_distribution AS
SELECT
    platform,
    sentiment,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY platform), 2) AS percentage,
    AVG(sentiment_score) AS avg_score
FROM sentiment_analysis
WHERE processed_at >= NOW() - INTERVAL '24 hours'
GROUP BY platform, sentiment
ORDER BY platform, count DESC;

-- Trending keywords summary
CREATE OR REPLACE VIEW v_trending_keywords AS
SELECT
    k.platform,
    k.keyword,
    SUM(k.count) AS total_count,
    MIN(k.rank) AS best_rank,
    COUNT(*) AS window_count
FROM keyword_trends k
WHERE k.window_start >= NOW() - INTERVAL '6 hours'
GROUP BY k.platform, k.keyword
HAVING SUM(k.count) > 5
ORDER BY total_count DESC
LIMIT 100;

-- Trending hashtags summary
CREATE OR REPLACE VIEW v_trending_hashtags AS
SELECT
    h.platform,
    h.hashtag,
    SUM(h.count) AS total_count,
    MIN(h.rank) AS best_rank,
    COUNT(*) AS window_count
FROM hashtag_trends h
WHERE h.window_start >= NOW() - INTERVAL '6 hours'
GROUP BY h.platform, h.hashtag
HAVING SUM(h.count) > 3
ORDER BY total_count DESC
LIMIT 100;

-- Alert summary
CREATE OR REPLACE VIEW v_recent_alerts AS
SELECT
    alert_type,
    platform,
    COUNT(*) AS alert_count,
    MAX(sent_at) AS last_alert,
    AVG(z_score) AS avg_zscore
FROM alert_history
WHERE sent_at >= NOW() - INTERVAL '24 hours'
GROUP BY alert_type, platform
ORDER BY alert_count DESC;

-- API quota status
CREATE OR REPLACE VIEW v_api_quota_status AS
SELECT
    platform,
    date,
    requests_made,
    quota_limit,
    ROUND((requests_made::decimal / quota_limit) * 100, 2) AS usage_percentage,
    quota_limit - requests_made AS remaining,
    last_updated
FROM api_quota_usage
WHERE date = CURRENT_DATE
ORDER BY usage_percentage DESC;

-- Daily comparison view
CREATE OR REPLACE VIEW v_daily_comparison AS
SELECT
    s.platform,
    s.date,
    s.total_views,
    LAG(s.total_views) OVER (PARTITION BY s.platform ORDER BY s.date) AS prev_day_views,
    CASE
        WHEN LAG(s.total_views) OVER (PARTITION BY s.platform ORDER BY s.date) > 0
        THEN ROUND(((s.total_views - LAG(s.total_views) OVER (PARTITION BY s.platform ORDER BY s.date))::decimal
            / LAG(s.total_views) OVER (PARTITION BY s.platform ORDER BY s.date)) * 100, 2)
        ELSE 0
    END AS views_growth_pct,
    s.total_content,
    s.avg_engagement_rate,
    s.trending_count
FROM daily_summary s
WHERE s.date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY s.date DESC, s.platform;

-- Content type distribution
CREATE OR REPLACE VIEW v_content_type_distribution AS
SELECT
    platform,
    content_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY platform), 2) AS percentage
FROM content_metadata
WHERE first_seen_at >= NOW() - INTERVAL '24 hours'
GROUP BY platform, content_type
ORDER BY platform, count DESC;

-- Real-time dashboard metrics
CREATE OR REPLACE VIEW v_dashboard_metrics AS
SELECT
    'platform_metrics' AS metric_type,
    platform,
    SUM(total_views) AS value,
    'views' AS metric_name,
    MAX(window_end) AS last_updated
FROM platform_metrics
WHERE window_start >= NOW() - INTERVAL '1 hour'
GROUP BY platform

UNION ALL

SELECT
    'engagement' AS metric_type,
    platform,
    AVG(avg_engagement_rate) AS value,
    'engagement_rate' AS metric_name,
    MAX(window_end) AS last_updated
FROM platform_metrics
WHERE window_start >= NOW() - INTERVAL '1 hour'
GROUP BY platform

UNION ALL

SELECT
    'trending' AS metric_type,
    platform,
    COUNT(*) AS value,
    'trending_count' AS metric_name,
    MAX(detected_at) AS last_updated
FROM trending_content
WHERE window_start >= NOW() - INTERVAL '1 hour'
  AND buzz_score > 0.5
GROUP BY platform;
