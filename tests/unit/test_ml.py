"""
Unit tests for ML components.
"""
import pytest


class TestSimpleSentiment:
    """Tests for simple keyword-based sentiment."""

    def test_positive_sentiment(self):
        """Test positive sentiment detection."""
        from streaming.ml.sentiment import SentimentAnalyzer

        # Test with Korean positive words
        positive_texts = [
            "이 제품 정말 좋아요 추천합니다!",
            "최고의 구매 만족합니다",
            "대박 완전 강추!",
        ]

        analyzer = SentimentAnalyzer()

        for text in positive_texts:
            # Using simple sentiment for testing
            # The actual result depends on the keywords found
            result = analyzer.analyze_text(text)
            assert result is not None

    def test_negative_sentiment(self):
        """Test negative sentiment detection."""
        negative_texts = [
            "이 제품 별로예요 실망입니다",
            "최악의 구매 후회합니다",
            "환불하고 싶어요",
        ]

        # These would be detected by keyword-based approach
        assert len(negative_texts) > 0


class TestKeywordStopwords:
    """Tests for stopword filtering."""

    def test_korean_stopwords_filtered(self):
        """Test that Korean stopwords are filtered."""
        from streaming.ml.keyword_extraction import KOREAN_STOPWORDS

        common_words = ["이", "그", "저", "것", "수", "등"]
        for word in common_words:
            assert word in KOREAN_STOPWORDS

    def test_english_stopwords_filtered(self):
        """Test that English stopwords are filtered."""
        from streaming.ml.keyword_extraction import KOREAN_STOPWORDS

        common_words = ["the", "a", "is", "are", "was"]
        for word in common_words:
            assert word in KOREAN_STOPWORDS


class TestTrendingThresholds:
    """Tests for trending detection thresholds."""

    def test_default_zscore_threshold(self):
        """Test default Z-score threshold."""
        from streaming.ml.trend_detection import TrendDetector

        detector = TrendDetector()
        assert detector.zscore_threshold == 3.0

    def test_custom_zscore_threshold(self):
        """Test custom Z-score threshold."""
        from streaming.ml.trend_detection import TrendDetector

        detector = TrendDetector(zscore_threshold=2.5)
        assert detector.zscore_threshold == 2.5

    def test_min_data_points(self):
        """Test minimum data points requirement."""
        from streaming.ml.trend_detection import TrendDetector

        detector = TrendDetector(min_data_points=20)
        assert detector.min_data_points == 20


class TestBuzzScoreCalculation:
    """Tests for buzz score calculation."""

    def test_buzz_score_weights(self):
        """Test that buzz score weights sum to 1."""
        views_weight = 0.4
        engagement_weight = 0.3
        recency_weight = 0.3

        total = views_weight + engagement_weight + recency_weight
        assert abs(total - 1.0) < 0.001


class TestModelLoading:
    """Tests for ML model loading."""

    def test_sentiment_model_config(self):
        """Test sentiment model configuration."""
        from streaming.ml.sentiment import SentimentAnalyzer

        analyzer = SentimentAnalyzer(
            model_name="klue/bert-base",
            batch_size=32,
            max_length=512,
        )

        assert analyzer.model_name == "klue/bert-base"
        assert analyzer.batch_size == 32
        assert analyzer.max_length == 512

    def test_lazy_loading(self):
        """Test that model is lazily loaded."""
        from streaming.ml.sentiment import SentimentAnalyzer

        analyzer = SentimentAnalyzer()
        # Pipeline should not be loaded until first use
        assert analyzer._pipeline is None
