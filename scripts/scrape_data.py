import praw
import pandas as pd
import time
import logging
from dotenv import dotenv_values
from models import Config

# Load environment variables from .env file
config = dotenv_values(".env")

# Load configuration from the Config class
config = Config(**config)

# Set up logging to stdout
logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

POSTS_LIMIT = 1000  # Set the limit for the number of posts to scrape

reddit = praw.Reddit(
    client_id=config.REDDIT_CLIENT_ID,
    client_secret=config.REDDIT_CLIENT_SECRET,
    user_agent=config.REDDIT_USER_AGENT,
    ratelimit_seconds=config.REDDIT_RATE_LIMIT,
)


subreddit = reddit.subreddit("eb_1a")

# Display the name of the Subreddit
print("Display Name:", subreddit.display_name)

# Display the title of the Subreddit
print("Title:", subreddit.title)

# Display the description of the Subreddit
print("Description:", subreddit.description)

data = []

logging.info("Scraping posts and comments from subreddit: %s", subreddit.display_name)
# Check if the subreddit is private

# Initialize a counter for checkpointing
checkpoint_counter = 0

# Scraping posts & Comments
for post in subreddit.new(limit=POSTS_LIMIT):
    data.append(
        {
            "Type": "Post",
            "Post_id": post.id,
            "Title": post.title,
            "Author": post.author.name if post.author else "Unknown",
            "Timestamp": post.created_utc,
            "Text": post.selftext,
            "Score": post.score,
            "Total_comments": post.num_comments,
            "Post_URL": post.url,
        }
    )
    checkpoint_counter += 1

    # Save checkpoint data every 10 posts
    if checkpoint_counter % 10 == 0:
        logging.info("Saving checkpoint data to CSV.")
        pd.DataFrame(data).to_csv("eb1a_threads_data.csv", mode='a', index=False, header=not pd.io.common.file_exists("eb1a_threads_data.csv"))

    time.sleep(2)  # Sleep for 2 seconds to avoid hitting the API too quickly
    # Log the post details
    logging.info(
        "Post ID: %s, Title: %s, Author: %s, Score: %d, Comments: %d",
        post.id,
        post.title,
        post.author.name if post.author else "Unknown",
        post.score,
        post.num_comments,
    )

    logging.info(
        "Sleeping for 2 seconds to avoid hitting the API too quickly before scraping comments."
    )
    # Check if the post has comments
    if post.num_comments > 0:
        # Scraping comments for each post
        post.comments.replace_more(limit=5)
        for comment in post.comments.list():
            data.append(
                {
                    "Type": "Comment",
                    "Post_id": post.id,
                    "Title": post.title,
                    "Author": comment.author.name if comment.author else "Unknown",
                    "Timestamp": pd.to_datetime(comment.created_utc, unit="s"),
                    "Text": comment.body,
                    "Score": comment.score,
                    "Total_comments": 0,  # Comments don't have this attribute
                    "Post_URL": None,  # Comments don't have this attribute
                }
            )

    # Sleep for 2 seconds before next post
    logging.info("Sleeping for 2 seconds before scraping the next post.")
    time.sleep(2)

# Final save of the DataFrame to a CSV file
logging.info("Saving final data to CSV.")
eb1a_threads_data = pd.DataFrame(data)
eb1a_threads_data.to_csv("eb1a_threads_data.csv", index=False)
