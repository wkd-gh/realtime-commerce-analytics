"""
Naver Shopping Data Producer for collecting product data.
"""
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from .base_producer import BaseProducer, API_QUOTA_REMAINING

logger = logging.getLogger(__name__)


class NaverShoppingProducer(BaseProducer):
    """
    Producer for Naver Shopping product data.

    Uses the Naver Search API to collect:
    - Product information
    - Prices and discounts
    - Seller information
    - Reviews (estimated)
    """

    API_URL = "https://openapi.naver.com/v1/search/shop.json"

    def __init__(
        self,
        kafka_config: Dict[str, Any],
        client_id: str,
        client_secret: str,
        keywords: Optional[List[str]] = None,
        categories: Optional[List[Dict]] = None,
        max_results: int = 100,
    ):
        """
        Initialize Naver Shopping producer.

        Args:
            kafka_config: Kafka configuration
            client_id: Naver API Client ID
            client_secret: Naver API Client Secret
            keywords: List of keywords to search
            categories: List of category configurations
            max_results: Maximum results per API call
        """
        super().__init__(
            kafka_config=kafka_config,
            topic_name='naver-shopping-raw',
            platform_name='naver_shopping',
        )

        self.client_id = client_id
        self.client_secret = client_secret
        self.keywords = keywords or []
        self.categories = categories or []
        self.max_results = min(max_results, 100)  # API limit

        # Rate limit tracking (25,000 per day)
        self.requests_made = 0
        self.rate_limit = 25000

        logger.info(
            f"Initialized Naver Shopping producer with {len(self.keywords)} keywords"
        )

    def _update_rate_limit(self):
        """Update rate limit tracking."""
        self.requests_made += 1
        remaining = self.rate_limit - self.requests_made
        API_QUOTA_REMAINING.labels(platform='naver_shopping').set(remaining)

    def _search_products(
        self,
        query: str,
        display: int = 100,
        start: int = 1,
        sort: str = 'sim',
    ) -> List[Dict]:
        """
        Search for products.

        Args:
            query: Search query
            display: Number of results (max 100)
            start: Start position
            sort: Sort order (sim, date, asc, dsc)

        Returns:
            List of product data
        """
        headers = {
            'X-Naver-Client-Id': self.client_id,
            'X-Naver-Client-Secret': self.client_secret,
        }

        params = {
            'query': query,
            'display': min(display, self.max_results),
            'start': start,
            'sort': sort,
        }

        try:
            response = requests.get(
                self.API_URL,
                headers=headers,
                params=params,
                timeout=10,
            )
            self._update_rate_limit()

            response.raise_for_status()
            data = response.json()

            return [{
                'item': item,
                'query': query,
                'total_results': data.get('total', 0),
            } for item in data.get('items', [])]

        except requests.RequestException as e:
            logger.error(f"Naver Shopping API error: {e}")
            return []

    def _search_by_category(self, category: Dict) -> List[Dict]:
        """
        Search products in a category.

        Args:
            category: Category configuration dict

        Returns:
            List of product data
        """
        results = []
        category_name = category.get('name', '')

        # Search with category name
        products = self._search_products(category_name, sort='date')
        for product in products:
            product['category'] = category
        results.extend(products)

        return results

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from Naver Shopping API.

        Returns:
            List of raw Naver Shopping data
        """
        all_data = []

        # Search by keywords
        for keyword in self.keywords:
            products = self._search_products(keyword)
            all_data.extend(products)

        # Search by categories
        for category in self.categories:
            products = self._search_by_category(category)
            all_data.extend(products)

        logger.info(f"Fetched {len(all_data)} items from Naver Shopping")
        return all_data

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw Naver Shopping data to standardized format.

        Args:
            raw_data: Raw Naver API response data

        Returns:
            Standardized data dictionary
        """
        item = raw_data.get('item', {})
        category = raw_data.get('category', {})

        # Parse prices
        low_price = int(item.get('lprice', 0))
        high_price = int(item.get('hprice', 0)) or low_price

        # Calculate discount if high price is available
        discount_rate = 0
        if high_price > low_price:
            discount_rate = round((high_price - low_price) / high_price * 100, 1)

        # Clean HTML tags from title
        import re
        title = re.sub(r'<[^>]+>', '', item.get('title', ''))

        # Extract brand from title or category
        brand = item.get('brand', '')
        maker = item.get('maker', '')

        return {
            'content_type': 'product',
            'content_id': item.get('productId'),
            'title': title,
            'description': item.get('image', ''),  # Using image URL as reference
            'link': item.get('link'),
            'image_url': item.get('image'),
            'low_price': low_price,
            'high_price': high_price,
            'discount_rate': discount_rate,
            'mall_name': item.get('mallName'),
            'product_type': item.get('productType'),  # 1: 일반상품, 2: 일반상품(가격비교), 3: 카탈로그, 4: 카탈로그(가격비교)
            'brand': brand,
            'maker': maker,
            'category1': item.get('category1', ''),
            'category2': item.get('category2', ''),
            'category3': item.get('category3', ''),
            'category4': item.get('category4', ''),
            'search_keyword': raw_data.get('query'),
            'search_category': category.get('name'),
            'total_results': raw_data.get('total_results', 0),
            'fetched_at': datetime.utcnow().isoformat(),
        }


def main():
    """Main entry point for Naver Shopping producer."""
    import yaml
    from dotenv import load_dotenv

    load_dotenv()

    # Load configuration
    with open('config/platforms.yaml', 'r') as f:
        config = yaml.safe_load(f)

    naver_config = config.get('naver_shopping', {})

    kafka_config = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    }

    # Initialize and run producer
    producer = NaverShoppingProducer(
        kafka_config=kafka_config,
        client_id=os.getenv('NAVER_CLIENT_ID'),
        client_secret=os.getenv('NAVER_CLIENT_SECRET'),
        keywords=naver_config.get('keywords', []),
        categories=naver_config.get('categories', []),
        max_results=naver_config.get('max_results_per_request', 100),
    )

    try:
        producer.start(interval_seconds=naver_config.get('polling_interval', 300))
    except KeyboardInterrupt:
        producer.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
