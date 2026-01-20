"""
Slack sink for real-time notifications.
"""
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

import requests
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

logger = logging.getLogger(__name__)


class SlackSink:
    """
    Slack sink for sending alerts and notifications.

    Supports:
    - Trending content alerts
    - Anomaly detection alerts
    - Daily summary reports
    """

    def __init__(
        self,
        webhook_url: str,
        channel: Optional[str] = None,
        username: str = "Analytics Bot",
        icon_emoji: str = ":chart_with_upwards_trend:",
    ):
        """
        Initialize Slack sink.

        Args:
            webhook_url: Slack webhook URL
            channel: Override channel (optional)
            username: Bot username
            icon_emoji: Bot emoji
        """
        self.webhook_url = webhook_url
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji

    def send_message(
        self,
        text: str,
        attachments: Optional[List[Dict]] = None,
        blocks: Optional[List[Dict]] = None,
    ) -> bool:
        """
        Send a message to Slack.

        Args:
            text: Message text
            attachments: Message attachments (optional)
            blocks: Block Kit blocks (optional)

        Returns:
            True if successful
        """
        if not self.webhook_url:
            logger.warning("Slack webhook URL not configured")
            return False

        try:
            payload = {
                "text": text,
                "username": self.username,
                "icon_emoji": self.icon_emoji,
            }

            if self.channel:
                payload["channel"] = self.channel

            if attachments:
                payload["attachments"] = attachments

            if blocks:
                payload["blocks"] = blocks

            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10,
            )

            if response.status_code == 200:
                logger.debug("Sent Slack message successfully")
                return True
            else:
                logger.error(f"Slack API error: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Failed to send Slack message: {e}")
            return False

    def send_trending_alert(
        self,
        content: Dict,
    ) -> bool:
        """
        Send alert for trending content.

        Args:
            content: Trending content data

        Returns:
            True if successful
        """
        platform = content.get("platform", "Unknown")
        title = content.get("title", "N/A")
        views = content.get("views", 0)
        z_score = content.get("z_score", 0)
        url = content.get("url", "")

        # Platform emoji mapping
        platform_emoji = {
            "youtube": ":youtube:",
            "twitter": ":twitter:",
            "tiktok": ":tiktok:",
            "naver_shopping": ":shopping_trolley:",
            "google_trends": ":chart_with_upwards_trend:",
        }

        emoji = platform_emoji.get(platform.lower(), ":fire:")

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} 급상승 콘텐츠 감지!",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*플랫폼:*\n{platform}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*조회수:*\n{views:,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Z-Score:*\n{z_score:.2f}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*트렌드 레벨:*\n{'🔥 Viral' if z_score > 5 else '🔥 Hot' if z_score > 4 else '📈 Trending'}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*제목:*\n{title[:100]}{'...' if len(title) > 100 else ''}"
                }
            },
        ]

        if url:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{url}|콘텐츠 보기>"
                }
            })

        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"감지 시간: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
                }
            ]
        })

        return self.send_message(
            text=f"급상승 콘텐츠 감지: {title}",
            blocks=blocks,
        )

    def send_anomaly_alert(
        self,
        metric_name: str,
        current_value: float,
        expected_value: float,
        z_score: float,
        platform: str,
    ) -> bool:
        """
        Send alert for anomaly detection.

        Args:
            metric_name: Name of the metric
            current_value: Current metric value
            expected_value: Expected (average) value
            z_score: Z-score of the anomaly
            platform: Platform name

        Returns:
            True if successful
        """
        is_positive = z_score > 0
        direction = "급증" if is_positive else "급감"
        color = "#2eb886" if is_positive else "#dc3545"

        attachments = [{
            "color": color,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f":warning: *이상 패턴 감지: {metric_name} {direction}*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*플랫폼:*\n{platform}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*현재 값:*\n{current_value:,.0f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*예상 값:*\n{expected_value:,.0f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*편차:*\n{z_score:.2f}σ"
                        }
                    ]
                }
            ]
        }]

        return self.send_message(
            text=f"이상 패턴 감지: {platform} - {metric_name} {direction}",
            attachments=attachments,
        )

    def send_daily_summary(
        self,
        summary_data: Dict,
    ) -> bool:
        """
        Send daily summary report.

        Args:
            summary_data: Summary statistics

        Returns:
            True if successful
        """
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📊 일일 분석 리포트",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{datetime.utcnow().strftime('%Y년 %m월 %d일')}* 분석 결과입니다."
                }
            },
            {"type": "divider"},
        ]

        # Platform metrics
        for platform, metrics in summary_data.get("platforms", {}).items():
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{platform.upper()}*"
                },
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"총 콘텐츠: {metrics.get('total_content', 0):,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"총 조회수: {metrics.get('total_views', 0):,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"평균 참여율: {metrics.get('avg_engagement', 0):.2f}%"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"트렌딩: {metrics.get('trending_count', 0)}개"
                    }
                ]
            })

        # Top content
        top_content = summary_data.get("top_content", [])
        if top_content:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*🏆 Top 5 콘텐츠*"
                }
            })

            for i, content in enumerate(top_content[:5], 1):
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{i}. [{content.get('platform', 'N/A')}] {content.get('title', 'N/A')[:50]} - 조회수: {content.get('views', 0):,}"
                    }
                })

        return self.send_message(
            text="일일 분석 리포트",
            blocks=blocks,
        )

    def create_foreach_batch_writer(
        self,
        zscore_threshold: float = 3.0,
    ):
        """
        Create a foreachBatch writer for alerts.

        Args:
            zscore_threshold: Z-score threshold for alerts

        Returns:
            foreachBatch function
        """
        webhook_url = self.webhook_url
        channel = self.channel
        username = self.username
        icon_emoji = self.icon_emoji

        def write_alerts(batch_df: DataFrame, batch_id: int):
            """Send alerts for trending content."""
            if batch_df.isEmpty():
                return

            if not webhook_url:
                return

            try:
                # Filter for trending content
                trending = batch_df.filter(
                    batch_df.z_score > zscore_threshold
                ).collect()

                for row in trending[:5]:  # Limit to 5 alerts per batch
                    content = row.asDict()

                    sink = SlackSink(
                        webhook_url=webhook_url,
                        channel=channel,
                        username=username,
                        icon_emoji=icon_emoji,
                    )
                    sink.send_trending_alert(content)

                if trending:
                    logger.info(f"Batch {batch_id}: Sent {len(trending[:5])} Slack alerts")

            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to send Slack alerts: {e}")

        return write_alerts

    def write_stream(
        self,
        df: DataFrame,
        checkpoint_location: str,
        zscore_threshold: float = 3.0,
        trigger_interval: str = "1 minute",
    ) -> StreamingQuery:
        """
        Write streaming alerts to Slack.

        Args:
            df: Streaming DataFrame with trending content
            checkpoint_location: Checkpoint directory
            zscore_threshold: Z-score threshold for alerts
            trigger_interval: Trigger processing interval

        Returns:
            StreamingQuery object
        """
        return df.writeStream \
            .foreachBatch(self.create_foreach_batch_writer(zscore_threshold)) \
            .outputMode("append") \
            .trigger(processingTime=trigger_interval) \
            .option("checkpointLocation", f"{checkpoint_location}/slack_alerts") \
            .start()
