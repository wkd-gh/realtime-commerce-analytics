"""
Keyword extraction for streaming data.
"""
import logging
import re
from collections import Counter
from typing import List, Dict, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, udf, explode, split, lower, regexp_replace, trim,
    collect_list, count, sum as spark_sum, window, current_timestamp,
    array_distinct, flatten, size, array
)
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, DoubleType

logger = logging.getLogger(__name__)


# Korean stopwords
KOREAN_STOPWORDS = set([
    "이", "그", "저", "것", "수", "등", "들", "및", "에", "에서", "의", "를", "을",
    "은", "는", "가", "이다", "있다", "하다", "되다", "않다", "없다", "같다",
    "그리고", "그러나", "하지만", "또한", "더", "매우", "아주", "정말", "너무",
    "좀", "잘", "많이", "조금", "약간", "거의", "모두", "전부", "다", "어떤",
    "무슨", "어느", "이런", "그런", "저런", "어떻게", "왜", "언제", "어디",
    "누구", "무엇", "얼마", "몇", "ㅋㅋ", "ㅎㅎ", "ㅠㅠ", "ㅜㅜ", "www",
    "http", "https", "com", "co", "kr", "the", "a", "an", "is", "are", "was",
    "were", "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "must", "shall",
])


class KeywordExtractor:
    """
    Keyword extractor using TF-IDF and frequency analysis.

    Supports both Korean and English text.
    """

    def __init__(
        self,
        min_word_length: int = 2,
        max_keywords: int = 10,
        use_konlpy: bool = True,
    ):
        """
        Initialize keyword extractor.

        Args:
            min_word_length: Minimum word length to consider
            max_keywords: Maximum keywords to extract
            use_konlpy: Whether to use KoNLPy for Korean morpheme analysis
        """
        self.min_word_length = min_word_length
        self.max_keywords = max_keywords
        self.use_konlpy = use_konlpy
        self._tagger = None

    def _get_tagger(self):
        """Lazy load KoNLPy tagger."""
        if self._tagger is None and self.use_konlpy:
            try:
                from konlpy.tag import Okt
                self._tagger = Okt()
                logger.info("Loaded KoNLPy Okt tagger")
            except ImportError:
                logger.warning("KoNLPy not available, using simple tokenization")
                self.use_konlpy = False
        return self._tagger

    def extract_nouns(self, text: str) -> List[str]:
        """
        Extract nouns from Korean text.

        Args:
            text: Text to analyze

        Returns:
            List of nouns
        """
        if not text:
            return []

        try:
            tagger = self._get_tagger()
            if tagger:
                nouns = tagger.nouns(text)
                return [n for n in nouns if len(n) >= self.min_word_length]
        except Exception as e:
            logger.warning(f"Noun extraction failed: {e}")

        # Fallback to simple word extraction
        return self._simple_tokenize(text)

    def _simple_tokenize(self, text: str) -> List[str]:
        """
        Simple tokenization without morpheme analysis.

        Args:
            text: Text to tokenize

        Returns:
            List of tokens
        """
        if not text:
            return []

        # Remove URLs
        text = re.sub(r'https?://\S+', '', text)

        # Remove special characters but keep Korean
        text = re.sub(r'[^\w\s\u3131-\u3163\uac00-\ud7a3]', ' ', text)

        # Split and filter
        words = text.lower().split()
        return [
            w for w in words
            if len(w) >= self.min_word_length
            and w not in KOREAN_STOPWORDS
        ]

    def extract_keywords(self, text: str) -> List[Tuple[str, float]]:
        """
        Extract keywords with scores from text.

        Args:
            text: Text to analyze

        Returns:
            List of (keyword, score) tuples
        """
        if not text:
            return []

        # Get nouns/tokens
        words = self.extract_nouns(text)

        # Count frequencies
        counter = Counter(words)

        # Return top keywords with normalized scores
        total = sum(counter.values())
        if total == 0:
            return []

        return [
            (word, count / total)
            for word, count in counter.most_common(self.max_keywords)
        ]

    @staticmethod
    def create_keyword_udf(max_keywords: int = 10):
        """
        Create Spark UDF for keyword extraction.

        Args:
            max_keywords: Maximum keywords to extract

        Returns:
            Spark UDF
        """
        @udf(returnType=ArrayType(StringType()))
        def extract_keywords_udf(text: str) -> List[str]:
            if not text:
                return []

            # Remove URLs
            text = re.sub(r'https?://\S+', '', text)

            # Remove special characters
            text = re.sub(r'[^\w\s\u3131-\u3163\uac00-\ud7a3]', ' ', text)

            # Split and filter
            words = text.lower().split()
            filtered = [
                w for w in words
                if len(w) >= 2 and w not in KOREAN_STOPWORDS
            ]

            # Count and return top keywords
            counter = Counter(filtered)
            return [word for word, _ in counter.most_common(max_keywords)]

        return extract_keywords_udf

    @staticmethod
    def add_keywords_column(
        df: DataFrame,
        text_column: str = "text",
        max_keywords: int = 10,
    ) -> DataFrame:
        """
        Add keywords column to DataFrame.

        Args:
            df: Input DataFrame
            text_column: Column with text to analyze
            max_keywords: Maximum keywords to extract

        Returns:
            DataFrame with keywords column
        """
        extract_udf = KeywordExtractor.create_keyword_udf(max_keywords)

        return df.withColumn(
            "extracted_keywords",
            extract_udf(col(text_column))
        )

    @staticmethod
    def aggregate_keywords(
        df: DataFrame,
        window_duration: str = "15 minutes",
        watermark_delay: str = "10 minutes",
        top_n: int = 20,
    ) -> DataFrame:
        """
        Aggregate keywords across windows.

        Args:
            df: DataFrame with keywords column
            window_duration: Window size
            watermark_delay: Watermark for late data
            top_n: Number of top keywords to keep

        Returns:
            Aggregated keywords DataFrame
        """
        # Explode keywords
        exploded = df.withWatermark("event_time", watermark_delay).select(
            col("event_time"),
            col("platform"),
            explode(col("extracted_keywords")).alias("keyword")
        )

        # Count by keyword
        keyword_counts = exploded.groupBy(
            window(col("event_time"), window_duration),
            col("platform"),
            col("keyword"),
        ).agg(
            count("*").alias("count")
        )

        # Rank and filter top keywords
        from pyspark.sql.window import Window
        rank_window = Window.partitionBy("window", "platform").orderBy(
            col("count").desc()
        )

        return keyword_counts.withColumn(
            "rank",
            row_number().over(rank_window)
        ).filter(
            col("rank") <= top_n
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "platform",
            "rank",
            "keyword",
            "count",
        ).withColumn(
            "extracted_at",
            current_timestamp()
        )

    @staticmethod
    def extract_hashtags(df: DataFrame, text_column: str = "text") -> DataFrame:
        """
        Extract hashtags from text.

        Args:
            df: Input DataFrame
            text_column: Column with text

        Returns:
            DataFrame with hashtags column
        """
        @udf(returnType=ArrayType(StringType()))
        def extract_hashtags_udf(text: str) -> List[str]:
            if not text:
                return []
            pattern = r'#[\w\u3131-\u3163\uac00-\ud7a3]+'
            return list(set(re.findall(pattern, text)))

        return df.withColumn(
            "hashtags",
            extract_hashtags_udf(col(text_column))
        )

    @staticmethod
    def aggregate_hashtags(
        df: DataFrame,
        window_duration: str = "15 minutes",
        watermark_delay: str = "10 minutes",
        top_n: int = 20,
    ) -> DataFrame:
        """
        Aggregate hashtags across windows.

        Args:
            df: DataFrame with hashtags column
            window_duration: Window size
            watermark_delay: Watermark for late data
            top_n: Number of top hashtags to keep

        Returns:
            Aggregated hashtags DataFrame
        """
        from pyspark.sql.functions import row_number
        from pyspark.sql.window import Window

        # Explode hashtags
        exploded = df.withWatermark("event_time", watermark_delay).select(
            col("event_time"),
            col("platform"),
            explode(col("hashtags")).alias("hashtag")
        )

        # Count by hashtag
        hashtag_counts = exploded.groupBy(
            window(col("event_time"), window_duration),
            col("platform"),
            col("hashtag"),
        ).agg(
            count("*").alias("count")
        )

        # Rank and filter top hashtags
        rank_window = Window.partitionBy("window", "platform").orderBy(
            col("count").desc()
        )

        return hashtag_counts.withColumn(
            "rank",
            row_number().over(rank_window)
        ).filter(
            col("rank") <= top_n
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "platform",
            "rank",
            "hashtag",
            "count",
        ).withColumn(
            "extracted_at",
            current_timestamp()
        )

    @staticmethod
    def generate_wordcloud_data(
        df: DataFrame,
        window_duration: str = "1 hour",
        watermark_delay: str = "10 minutes",
    ) -> DataFrame:
        """
        Generate data for word cloud visualization.

        Args:
            df: DataFrame with keywords
            window_duration: Window size
            watermark_delay: Watermark for late data

        Returns:
            DataFrame with word cloud data
        """
        from pyspark.sql.functions import row_number
        from pyspark.sql.window import Window

        # Explode and count keywords
        exploded = df.withWatermark("event_time", watermark_delay).select(
            col("event_time"),
            col("platform"),
            explode(col("extracted_keywords")).alias("word")
        )

        word_counts = exploded.groupBy(
            window(col("event_time"), window_duration),
            col("platform"),
            col("word"),
        ).agg(
            count("*").alias("frequency")
        )

        # Calculate relative size for visualization
        total_window = Window.partitionBy("window", "platform")

        return word_counts.withColumn(
            "total_words",
            spark_sum("frequency").over(total_window)
        ).withColumn(
            "weight",
            col("frequency") / col("total_words")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "platform",
            "word",
            "frequency",
            "weight",
        ).orderBy(
            col("window_start").desc(),
            col("platform"),
            col("frequency").desc()
        )
