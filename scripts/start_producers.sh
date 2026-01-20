#!/bin/bash
set -e

echo "Starting all producers..."

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '#' | xargs)
fi

# Create logs directory
mkdir -p logs/producers

# Start producers in background
echo "Starting YouTube producer..."
python -m producers.youtube_producer > logs/producers/youtube.log 2>&1 &
echo $! > logs/producers/youtube.pid

echo "Starting Twitter producer..."
python -m producers.twitter_producer > logs/producers/twitter.log 2>&1 &
echo $! > logs/producers/twitter.pid

echo "Starting TikTok producer..."
python -m producers.tiktok_producer > logs/producers/tiktok.log 2>&1 &
echo $! > logs/producers/tiktok.pid

echo "Starting Naver Shopping producer..."
python -m producers.naver_shopping_producer > logs/producers/naver.log 2>&1 &
echo $! > logs/producers/naver.pid

echo "Starting Google Trends producer..."
python -m producers.google_trends_producer > logs/producers/trends.log 2>&1 &
echo $! > logs/producers/trends.pid

echo ""
echo "All producers started!"
echo ""
echo "View logs:"
echo "  tail -f logs/producers/youtube.log"
echo "  tail -f logs/producers/twitter.log"
echo "  tail -f logs/producers/tiktok.log"
echo "  tail -f logs/producers/naver.log"
echo "  tail -f logs/producers/trends.log"
echo ""
echo "Stop all producers:"
echo "  kill \$(cat logs/producers/*.pid)"
