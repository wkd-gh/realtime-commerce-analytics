"""
Sentiment analysis for streaming data.
"""
import logging
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, udf, when, lit, length, current_timestamp,
    pandas_udf, PandasUDFType
)
from pyspark.sql.types import (
    StringType, DoubleType, StructType, StructField, ArrayType
)

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """
    Sentiment analyzer using Korean BERT model.

    Uses klue/bert-base for Korean text sentiment analysis.
    Supports both batch and streaming operations.
    """

    def __init__(
        self,
        model_name: str = "klue/bert-base",
        batch_size: int = 32,
        max_length: int = 512,
        use_gpu: bool = False,
    ):
        """
        Initialize sentiment analyzer.

        Args:
            model_name: HuggingFace model name
            batch_size: Batch size for inference
            max_length: Maximum text length
            use_gpu: Whether to use GPU
        """
        self.model_name = model_name
        self.batch_size = batch_size
        self.max_length = max_length
        self.use_gpu = use_gpu
        self._pipeline = None

    def _get_pipeline(self):
        """Lazy load the sentiment pipeline."""
        if self._pipeline is None:
            try:
                from transformers import pipeline

                device = 0 if self.use_gpu else -1
                self._pipeline = pipeline(
                    "sentiment-analysis",
                    model=self.model_name,
                    device=device,
                    truncation=True,
                    max_length=self.max_length,
                )
                logger.info(f"Loaded sentiment model: {self.model_name}")
            except Exception as e:
                logger.error(f"Failed to load sentiment model: {e}")
                raise
        return self._pipeline

    def analyze_text(self, text: str) -> Tuple[str, float]:
        """
        Analyze sentiment of a single text.

        Args:
            text: Text to analyze

        Returns:
            Tuple of (sentiment_label, confidence_score)
        """
        if not text or len(text.strip()) < 3:
            return ("neutral", 0.5)

        try:
            pipeline = self._get_pipeline()
            result = pipeline(text[:self.max_length])[0]

            label = result['label'].lower()
            score = result['score']

            # Normalize labels
            if label in ('positive', 'pos', '1', 'label_1'):
                return ('positive', score)
            elif label in ('negative', 'neg', '0', 'label_0'):
                return ('negative', score)
            else:
                return ('neutral', score)

        except Exception as e:
            logger.warning(f"Sentiment analysis failed: {e}")
            return ('neutral', 0.5)

    def analyze_batch(self, texts: List[str]) -> List[Tuple[str, float]]:
        """
        Analyze sentiment of multiple texts.

        Args:
            texts: List of texts to analyze

        Returns:
            List of (sentiment_label, confidence_score) tuples
        """
        results = []

        for text in texts:
            results.append(self.analyze_text(text))

        return results

    @staticmethod
    def create_sentiment_udf(model_name: str = "klue/bert-base"):
        """
        Create a Spark UDF for sentiment analysis.

        Args:
            model_name: HuggingFace model name

        Returns:
            Spark UDF function
        """
        # Define result schema
        result_schema = StructType([
            StructField("sentiment", StringType(), False),
            StructField("confidence", DoubleType(), False),
        ])

        @udf(returnType=result_schema)
        def analyze_sentiment(text: str):
            """UDF wrapper for sentiment analysis."""
            if not text or len(text.strip()) < 3:
                return ("neutral", 0.5)

            try:
                # Import inside UDF for serialization
                from transformers import pipeline

                # Use a simpler model for efficiency
                analyzer = pipeline(
                    "sentiment-analysis",
                    model=model_name,
                    device=-1,  # CPU
                    truncation=True,
                    max_length=512,
                )

                result = analyzer(text[:512])[0]
                label = result['label'].lower()
                score = float(result['score'])

                if label in ('positive', 'pos', '1', 'label_1'):
                    return ('positive', score)
                elif label in ('negative', 'neg', '0', 'label_0'):
                    return ('negative', score)
                else:
                    return ('neutral', score)

            except Exception:
                return ('neutral', 0.5)

        return analyze_sentiment

    @staticmethod
    def add_sentiment_column(
        df: DataFrame,
        text_column: str = "text",
        model_name: str = "klue/bert-base",
    ) -> DataFrame:
        """
        Add sentiment analysis columns to DataFrame.

        Args:
            df: Input DataFrame
            text_column: Name of text column to analyze
            model_name: HuggingFace model name

        Returns:
            DataFrame with sentiment columns
        """
        sentiment_udf = SentimentAnalyzer.create_sentiment_udf(model_name)

        return df.withColumn(
            "_sentiment_result",
            when(
                col(text_column).isNotNull() & (length(col(text_column)) > 3),
                sentiment_udf(col(text_column))
            ).otherwise(
                lit(None)
            )
        ).withColumn(
            "sentiment",
            when(col("_sentiment_result").isNotNull(),
                 col("_sentiment_result.sentiment"))
            .otherwise(lit("neutral"))
        ).withColumn(
            "sentiment_confidence",
            when(col("_sentiment_result").isNotNull(),
                 col("_sentiment_result.confidence"))
            .otherwise(lit(0.5))
        ).drop("_sentiment_result").withColumn(
            "sentiment_analyzed_at",
            current_timestamp()
        )

    @staticmethod
    def add_simple_sentiment(df: DataFrame, text_column: str = "text") -> DataFrame:
        """
        Add simple keyword-based sentiment (faster, no ML).

        Args:
            df: Input DataFrame
            text_column: Name of text column

        Returns:
            DataFrame with sentiment columns
        """
        # Positive keywords (Korean)
        positive_words = [
            "좋아", "최고", "추천", "만족", "대박", "짱", "굿", "예쁘", "이쁘",
            "감사", "행복", "훌륭", "완벽", "사랑", "좋은", "멋지", "최애", "갓",
            "꿀템", "존맛", "꿀잼", "신세계", "강추", "혜자"
        ]

        # Negative keywords (Korean)
        negative_words = [
            "별로", "최악", "실망", "후회", "싫어", "안좋", "나쁜", "불만",
            "짜증", "화나", "실패", "망", "노답", "쓰레기", "거지같", "최저",
            "환불", "사기", "노잼", "비추"
        ]

        # Create pattern conditions
        from pyspark.sql.functions import lower, array_contains, split, size, array

        positive_array = array([lit(w) for w in positive_words])
        negative_array = array([lit(w) for w in negative_words])

        @udf(returnType=StringType())
        def simple_sentiment(text: str) -> str:
            if not text:
                return "neutral"

            text_lower = text.lower()
            pos_count = sum(1 for w in positive_words if w in text_lower)
            neg_count = sum(1 for w in negative_words if w in text_lower)

            if pos_count > neg_count:
                return "positive"
            elif neg_count > pos_count:
                return "negative"
            else:
                return "neutral"

        @udf(returnType=DoubleType())
        def simple_sentiment_score(text: str) -> float:
            if not text:
                return 0.5

            text_lower = text.lower()
            pos_count = sum(1 for w in positive_words if w in text_lower)
            neg_count = sum(1 for w in negative_words if w in text_lower)
            total = pos_count + neg_count

            if total == 0:
                return 0.5

            return pos_count / total

        return df.withColumn(
            "sentiment",
            when(col(text_column).isNotNull(), simple_sentiment(col(text_column)))
            .otherwise(lit("neutral"))
        ).withColumn(
            "sentiment_score",
            when(col(text_column).isNotNull(), simple_sentiment_score(col(text_column)))
            .otherwise(lit(0.5))
        ).withColumn(
            "sentiment_analyzed_at",
            current_timestamp()
        )
