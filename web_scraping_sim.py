import requests
from datetime import datetime
import random

# Simulated financial news headlines with sentiment scores
# (-1 = negative, 0 = neutral, 1 = positive)
news_data = [
    {"headline": "Federal Reserve holds interest rates steady", "sentiment": 0.2},
    {"headline": "Stock market reaches all-time high", "sentiment": 0.9},
    {"headline": "Inflation concerns grow among economists", "sentiment": -0.7},
    {"headline": "Tech sector shows strong quarterly earnings", "sentiment": 0.6},
    {"headline": "Global supply chain disruptions continue", "sentiment": -0.5},
]

print("📰 Simulating financial news scraping...")
print(f"Scraped {len(news_data)} headlines at {datetime.now()}")
print("\nLatest Financial News:")
print("-" * 50)

for i, news in enumerate(news_data, 1):
    sentiment_icon = "📈" if news["sentiment"] > 0.3 else "📉" if news["sentiment"] < -0.3 else "➖"
    print(f"{i}. {sentiment_icon} {news['headline']} (Sentiment: {news['sentiment']:.1f})")

# Calculate average sentiment
avg_sentiment = sum(n["sentiment"] for n in news_data) / len(news_data)
print("-" * 50)
print(f"Overall Market Sentiment: {avg_sentiment:.2f}")

# Save to file (simulating data collection)
with open("financial_news.csv", "w") as f:
    f.write("timestamp,headline,sentiment\n")
    for news in news_data:
        f.write(f"{datetime.now()},{news['headline']},{news['sentiment']}\n")

print(f"\n✅ Data saved to 'financial_news.csv'")
