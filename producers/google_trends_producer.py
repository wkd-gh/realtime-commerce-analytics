"""
Google Trends Data Producer for collecting trend data.
"""
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pytrends.request import TrendReq

from .base_producer import BaseProducer

logger = logging.getLogger(__name__)


class GoogleTrendsProducer(BaseProducer):
    """
    Producer for Google Trends data.

    Uses pytrends to collect:
    - Interest over time
    - Interest by region
    - Related queries
    - Related topics
    """

    def __init__(
        self,
        kafka_config: Dict[str, Any],
        keywords: Optional[List[str]] = None,
        geo: str = 'KR',
        timeframe: str = 'now 1-H',
        rate_limit_delay: float = 1.0,
    ):
        """
        Initialize Google Trends producer.

        Args:
            kafka_config: Kafka configuration
            keywords: List of keywords to track
            geo: Geographic location code
            timeframe: Time range for trends
            rate_limit_delay: Delay between requests
        """
        super().__init__(
            kafka_config=kafka_config,
            topic_name='google-trends-raw',
            platform_name='google_trends',
        )

        self.keywords = keywords or []
        self.geo = geo
        self.timeframe = timeframe
        self.rate_limit_delay = rate_limit_delay

        # Initialize pytrends
        self.pytrends = TrendReq(hl='ko', tz=540)  # Korean, UTC+9

        logger.info(
            f"Initialized Google Trends producer with {len(self.keywords)} keywords"
        )

    def _get_interest_over_time(self, keywords: List[str]) -> Dict:
        """
        Get interest over time for keywords.

        Args:
            keywords: List of keywords (max 5)

        Returns:
            Interest over time data
        """
        try:
            # pytrends accepts max 5 keywords at once
            keywords = keywords[:5]

            self.pytrends.build_payload(
                keywords,
                cat=0,
                timeframe=self.timeframe,
                geo=self.geo,
                gprop='',
            )

            df = self.pytrends.interest_over_time()

            if df.empty:
                return {}

            # Convert DataFrame to dict
            result = {
                'keywords': keywords,
                'data': [],
            }

            for idx, row in df.iterrows():
                data_point = {
                    'timestamp': idx.isoformat(),
                    'values': {},
                }
                for keyword in keywords:
                    if keyword in df.columns:
                        data_point['values'][keyword] = int(row[keyword])

                if 'isPartial' in df.columns:
                    data_point['is_partial'] = bool(row['isPartial'])

                result['data'].append(data_point)

            return result

        except Exception as e:
            logger.error(f"Google Trends interest over time error: {e}")
            return {}

    def _get_interest_by_region(self, keyword: str) -> List[Dict]:
        """
        Get interest by region for a keyword.

        Args:
            keyword: Keyword to analyze

        Returns:
            Interest by region data
        """
        try:
            self.pytrends.build_payload(
                [keyword],
                cat=0,
                timeframe=self.timeframe,
                geo=self.geo,
                gprop='',
            )

            df = self.pytrends.interest_by_region(
                resolution='COUNTRY',
                inc_low_vol=True,
                inc_geo_code=True,
            )

            if df.empty:
                return []

            results = []
            for region, row in df.iterrows():
                results.append({
                    'region': region,
                    'geo_code': row.get('geoCode', ''),
                    'interest': int(row[keyword]) if keyword in df.columns else 0,
                })

            return results

        except Exception as e:
            logger.error(f"Google Trends interest by region error: {e}")
            return []

    def _get_related_queries(self, keyword: str) -> Dict:
        """
        Get related queries for a keyword.

        Args:
            keyword: Keyword to analyze

        Returns:
            Related queries data
        """
        try:
            self.pytrends.build_payload(
                [keyword],
                cat=0,
                timeframe=self.timeframe,
                geo=self.geo,
                gprop='',
            )

            related = self.pytrends.related_queries()

            result = {
                'top': [],
                'rising': [],
            }

            if keyword in related:
                keyword_data = related[keyword]

                # Top queries
                if keyword_data.get('top') is not None:
                    for _, row in keyword_data['top'].iterrows():
                        result['top'].append({
                            'query': row['query'],
                            'value': int(row['value']),
                        })

                # Rising queries
                if keyword_data.get('rising') is not None:
                    for _, row in keyword_data['rising'].iterrows():
                        result['rising'].append({
                            'query': row['query'],
                            'value': str(row['value']),  # Can be "Breakout"
                        })

            return result

        except Exception as e:
            logger.error(f"Google Trends related queries error: {e}")
            return {'top': [], 'rising': []}

    def _get_trending_searches(self) -> List[Dict]:
        """
        Get daily trending searches.

        Returns:
            List of trending search data
        """
        try:
            df = self.pytrends.trending_searches(pn='south_korea')

            results = []
            for idx, row in df.iterrows():
                results.append({
                    'rank': idx + 1,
                    'query': row[0],
                })

            return results

        except Exception as e:
            logger.error(f"Google Trends trending searches error: {e}")
            return []

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from Google Trends.

        Returns:
            List of raw Google Trends data
        """
        all_data = []

        # Get interest over time for all keywords (in batches of 5)
        for i in range(0, len(self.keywords), 5):
            batch = self.keywords[i:i+5]
            interest_data = self._get_interest_over_time(batch)

            if interest_data:
                all_data.append({
                    'type': 'interest_over_time',
                    'data': interest_data,
                })

            time.sleep(self.rate_limit_delay)

        # Get related queries for each keyword
        for keyword in self.keywords:
            related = self._get_related_queries(keyword)

            if related.get('top') or related.get('rising'):
                all_data.append({
                    'type': 'related_queries',
                    'keyword': keyword,
                    'data': related,
                })

            time.sleep(self.rate_limit_delay)

        # Get trending searches
        trending = self._get_trending_searches()
        if trending:
            all_data.append({
                'type': 'trending_searches',
                'data': trending,
            })

        logger.info(f"Fetched {len(all_data)} items from Google Trends")
        return all_data

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Google Trends data to standardized format.

        Args:
            raw_data: Raw Google Trends data

        Returns:
            Standardized data dictionary
        """
        data_type = raw_data.get('type')

        if data_type == 'interest_over_time':
            return self._transform_interest_over_time(raw_data)
        elif data_type == 'related_queries':
            return self._transform_related_queries(raw_data)
        elif data_type == 'trending_searches':
            return self._transform_trending_searches(raw_data)

        return raw_data

    def _transform_interest_over_time(self, raw_data: Dict) -> Dict:
        """Transform interest over time data."""
        data = raw_data.get('data', {})

        return {
            'content_type': 'interest_over_time',
            'content_id': f"iot_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            'keywords': data.get('keywords', []),
            'geo': self.geo,
            'timeframe': self.timeframe,
            'data_points': data.get('data', []),
            'fetched_at': datetime.utcnow().isoformat(),
        }

    def _transform_related_queries(self, raw_data: Dict) -> Dict:
        """Transform related queries data."""
        data = raw_data.get('data', {})
        keyword = raw_data.get('keyword', '')

        return {
            'content_type': 'related_queries',
            'content_id': f"rq_{keyword}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            'keyword': keyword,
            'geo': self.geo,
            'top_queries': data.get('top', []),
            'rising_queries': data.get('rising', []),
            'fetched_at': datetime.utcnow().isoformat(),
        }

    def _transform_trending_searches(self, raw_data: Dict) -> Dict:
        """Transform trending searches data."""
        data = raw_data.get('data', [])

        return {
            'content_type': 'trending_searches',
            'content_id': f"ts_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            'geo': self.geo,
            'trends': data,
            'fetched_at': datetime.utcnow().isoformat(),
        }


def main():
    """Main entry point for Google Trends producer."""
    import yaml
    from dotenv import load_dotenv

    load_dotenv()

    # Load configuration
    with open('config/platforms.yaml', 'r') as f:
        config = yaml.safe_load(f)

    trends_config = config.get('google_trends', {})

    kafka_config = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    }

    # Initialize and run producer
    producer = GoogleTrendsProducer(
        kafka_config=kafka_config,
        keywords=trends_config.get('keywords', []),
        geo=trends_config.get('geo', 'KR'),
        timeframe=trends_config.get('timeframe', 'now 1-H'),
    )

    try:
        producer.start(interval_seconds=trends_config.get('polling_interval', 600))
    except KeyboardInterrupt:
        producer.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
