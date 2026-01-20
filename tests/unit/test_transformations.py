"""
Unit tests for streaming transformations.
"""
import pytest
from unittest.mock import MagicMock


class TestDataEnrichment:
    """Tests for DataEnrichment class."""

    def test_extract_brand_from_text(self):
        """Test brand extraction from text."""
        from streaming.transformations.enrichment import DataEnrichment

        # Test Korean brand name
        text = "애플 아이폰 15 리뷰입니다"
        brand = DataEnrichment.extract_brand(text)
        assert brand == "Apple"

        # Test English brand name
        text = "New Nike shoes review"
        brand = DataEnrichment.extract_brand(text)
        assert brand == "Nike"

        # Test no brand
        text = "일반적인 텍스트입니다"
        brand = DataEnrichment.extract_brand(text)
        assert brand is None

    def test_extract_category_from_text(self):
        """Test category extraction from text."""
        from streaming.transformations.enrichment import DataEnrichment

        # Test beauty category
        text = "뷰티 제품 화장품 리뷰"
        category = DataEnrichment.extract_category(text)
        assert category == "Beauty"

        # Test electronics category
        text = "최신 스마트폰 테크 리뷰"
        category = DataEnrichment.extract_category(text)
        assert category == "Electronics"

    def test_extract_hashtags(self):
        """Test hashtag extraction."""
        from streaming.transformations.enrichment import DataEnrichment

        text = "#쿠팡추천 이 제품 좋아요 #베스트셀러 #추천"
        hashtags = DataEnrichment.extract_hashtags(text)

        assert len(hashtags) == 3
        assert "#쿠팡추천" in hashtags
        assert "#베스트셀러" in hashtags
        assert "#추천" in hashtags

    def test_extract_mentions(self):
        """Test mention extraction."""
        from streaming.transformations.enrichment import DataEnrichment

        text = "Hey @user1 check this out @user2"
        mentions = DataEnrichment.extract_mentions(text)

        assert len(mentions) == 2
        assert "@user1" in mentions
        assert "@user2" in mentions


class TestKeywordExtractor:
    """Tests for KeywordExtractor class."""

    def test_simple_tokenize(self):
        """Test simple tokenization."""
        from streaming.ml.keyword_extraction import KeywordExtractor

        extractor = KeywordExtractor(use_konlpy=False)
        text = "이것은 테스트 문장입니다 테스트"
        tokens = extractor._simple_tokenize(text)

        assert "테스트" in tokens
        assert len([t for t in tokens if t == "테스트"]) == 2

    def test_extract_keywords_returns_list(self):
        """Test keyword extraction returns list."""
        from streaming.ml.keyword_extraction import KeywordExtractor

        extractor = KeywordExtractor(use_konlpy=False, max_keywords=5)
        text = "애플 아이폰 아이폰 아이폰 삼성 갤럭시"
        keywords = extractor.extract_keywords(text)

        assert len(keywords) <= 5
        assert len(keywords) > 0

    def test_extract_keywords_empty_text(self):
        """Test keyword extraction with empty text."""
        from streaming.ml.keyword_extraction import KeywordExtractor

        extractor = KeywordExtractor(use_konlpy=False)
        keywords = extractor.extract_keywords("")

        assert keywords == []


class TestSentimentAnalyzer:
    """Tests for SentimentAnalyzer class."""

    def test_analyze_empty_text(self):
        """Test sentiment analysis with empty text."""
        from streaming.ml.sentiment import SentimentAnalyzer

        analyzer = SentimentAnalyzer()
        sentiment, score = analyzer.analyze_text("")

        assert sentiment == "neutral"
        assert score == 0.5

    def test_analyze_short_text(self):
        """Test sentiment analysis with very short text."""
        from streaming.ml.sentiment import SentimentAnalyzer

        analyzer = SentimentAnalyzer()
        sentiment, score = analyzer.analyze_text("ab")

        assert sentiment == "neutral"
        assert score == 0.5


class TestTrendDetector:
    """Tests for TrendDetector class."""

    @pytest.mark.skipif(True, reason="Requires PySpark")
    def test_detect_trending_content(self, spark_session):
        """Test trending content detection."""
        from streaming.ml.trend_detection import TrendDetector
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

        # Create test DataFrame
        schema = StructType([
            StructField("content_id", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("views", LongType(), True),
            StructField("z_score", DoubleType(), True),
        ])

        data = [
            ("1", "youtube", 10000, 4.5),  # Trending
            ("2", "youtube", 500, 0.5),    # Not trending
            ("3", "youtube", 50000, 6.0),  # Viral
        ]

        df = spark_session.createDataFrame(data, schema)
        result = TrendDetector.detect_trending_content(df, zscore_threshold=3.0)

        trending_count = result.filter(result.is_trending == True).count()
        assert trending_count == 2

        viral_count = result.filter(result.trend_level == "viral").count()
        assert viral_count == 1


class TestWindowAggregations:
    """Tests for WindowAggregations class."""

    @pytest.mark.skipif(True, reason="Requires PySpark with streaming")
    def test_aggregate_by_platform(self, spark_session):
        """Test platform aggregation."""
        from streaming.transformations.aggregations import WindowAggregations
        from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
        from datetime import datetime

        schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("platform", StringType(), True),
            StructField("views", LongType(), True),
            StructField("likes", LongType(), True),
            StructField("comments", LongType(), True),
            StructField("shares", LongType(), True),
            StructField("engagement_rate", DoubleType(), True),
            StructField("author_id", StringType(), True),
        ])

        # Test data
        now = datetime.now()
        data = [
            (now, "youtube", 1000, 100, 10, 5, 1.5, "author1"),
            (now, "youtube", 2000, 200, 20, 10, 2.0, "author2"),
            (now, "twitter", 500, 50, 5, 2, 1.0, "author3"),
        ]

        df = spark_session.createDataFrame(data, schema)
        # Note: This would need to be a streaming DataFrame for full testing
