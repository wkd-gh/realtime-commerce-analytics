"""
Sink modules for writing streaming data to various destinations.
"""
from .postgres_sink import PostgresSink
from .clickhouse_sink import ClickHouseSink
from .redis_sink import RedisSink
from .elasticsearch_sink import ElasticsearchSink
from .slack_sink import SlackSink

__all__ = [
    'PostgresSink',
    'ClickHouseSink',
    'RedisSink',
    'ElasticsearchSink',
    'SlackSink',
]
