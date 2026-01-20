"""
ML modules for streaming analytics.
"""
from .sentiment import SentimentAnalyzer
from .trend_detection import TrendDetector
from .keyword_extraction import KeywordExtractor

__all__ = ['SentimentAnalyzer', 'TrendDetector', 'KeywordExtractor']
