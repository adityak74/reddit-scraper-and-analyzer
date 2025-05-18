"""
## Reddit Data Analysis DAG

This DAG connects to the DuckDB database created by the scrape_reddit_and_load DAG
and uses the Airflow AI SDK with Ollama to perform conversation analysis on the Reddit data.
The sentiment analysis results are stored directly in the DuckDB database for further analysis.

The DAG uses the following tools:
- DuckDB for querying Reddit posts and comments and storing sentiment analysis
- Airflow AI SDK with Ollama to analyze conversations and sentiment
- Pandas for data processing and visualization

The DAG produces:
1. Summary statistics about the Reddit conversations
2. Sentiment analysis of the posts and comments (stored in DuckDB)
3. Topic identification and clustering of discussions (stored in DuckDB)
4. Key insights and sentiment trend summary

Dependencies:
- duckdb
- pandas
- ollama
- airflow-ai-sdk
- pydantic_ai
"""

import pendulum
import logging
import os
import json
import pandas as pd
import duckdb
from dotenv import load_dotenv
from models import Config

from airflow.exceptions import AirflowException, AirflowSkipException
from datetime import datetime, timedelta

from airflow.decorators import dag, task
import airflow_ai_sdk as ai_sdk
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

# Configure the model to use Ollama
model = OpenAIModel(
    model_name="llama3.1:8b",
    provider=OpenAIProvider(
        # Using the localhost URL since Ollama is running locally
        base_url="http://host.docker.internal:11434/v1"
    )
)

# Define the default_args for the DAG
default_args = {
    'owner': 'Astro',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Define schemas for AI analysis results
class SentimentAnalysis(ai_sdk.BaseModel):
    sentiment: str  # POSITIVE, NEGATIVE, NEUTRAL
    confidence: float
    reasoning: str

class TopicAnalysis(ai_sdk.BaseModel):
    main_topic: str
    subtopics: list[str]
    keywords: list[str]

class RedditInsights(ai_sdk.BaseModel):
    summary: str
    sentiment_trend: str
    key_topics: list[str]
    recommendations: list[str]

@task
def check_ai_dependencies():
    """
    Check if all required dependencies are installed for AI analysis
    """
    required_packages = ['duckdb', 'pandas', 'airflow_ai_sdk', 'pydantic_ai']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        error_msg = f"Missing required packages: {', '.join(missing_packages)}. Please install them via requirements.txt"
        logging.error(error_msg)
        raise ImportError(error_msg)
    
    # Log information about the environment
    airflow_home = os.environ.get('AIRFLOW_HOME', '.')
    dags_folder = os.path.join(airflow_home, 'dags')
    
    logging.info(f"Airflow Home: {airflow_home}")
    logging.info(f"DAGs Folder: {dags_folder}")
    logging.info(f"Current Working Directory: {os.getcwd()}")
    
    return "All AI dependencies are installed"

@task
def initialize_sentiment_table(data_summary):
    """
    Initialize or update the sentiment analysis table in DuckDB
    """
    try:
        db_path = data_summary['db_path']
        conn = duckdb.connect(db_path)
        
        # Check if sentiment table exists, create it if not
        table_exists = conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='post_sentiment'
        """).fetchone()
        
        if not table_exists:
            logging.info("Creating post_sentiment table in DuckDB")
            conn.execute("""
                CREATE TABLE post_sentiment (
                    Post_id VARCHAR PRIMARY KEY,
                    Sentiment VARCHAR,
                    Confidence FLOAT,
                    Reasoning TEXT,
                    Main_Topic VARCHAR,
                    Subtopics VARCHAR,
                    Keywords VARCHAR,
                    Analysis_Date TIMESTAMP
                )
            """)
        else:
            logging.info("post_sentiment table already exists in DuckDB")
            
        conn.close()
        return "Sentiment table initialized"
    except Exception as e:
        logging.error(f"Failed to initialize sentiment table: {e}")
        raise

@task
def load_duckdb_data():
    """
    Connect to DuckDB and load the Reddit data for analysis
    """
    try:
        # Set paths for DuckDB files
        airflow_home = os.environ.get('AIRFLOW_HOME', '.')
        container_db_path = os.path.join(airflow_home, 'data', 'eb1a_threads_data.duckdb')
        host_db_path = os.path.join(airflow_home, 'dags', 'data', 'eb1a_threads_data.duckdb')
        project_root_db_path = os.path.join(airflow_home, 'eb1a_threads_data.duckdb')
        
        # Try each path until we find one that exists
        for db_path in [host_db_path, container_db_path, project_root_db_path]:
            if os.path.exists(db_path):
                logging.info(f"Found DuckDB file at: {db_path}")
                conn = duckdb.connect(db_path)
                
                # Get post and comment counts
                post_count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
                comment_count = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
                
                logging.info(f"Connected to DuckDB with {post_count} posts and {comment_count} comments")
                
                # Get the top 5 posts by score
                top_posts = conn.execute("""
                    SELECT Post_id, Title, Author, Score, Total_comments
                    FROM posts
                    ORDER BY Score DESC
                    LIMIT 5
                """).fetchall()
                
                # Get the top 5 comments by score
                top_comments = conn.execute("""
                    SELECT Comment_id, Post_id, Author, Score, Text
                    FROM comments
                    ORDER BY Score DESC
                    LIMIT 5
                """).fetchall()
                
                # Get posts with the most comments
                most_discussed = conn.execute("""
                    SELECT Post_id, Title, Author, Score, Total_comments
                    FROM posts
                    ORDER BY Total_comments DESC
                    LIMIT 5
                """).fetchall()
                
                # Store the query results
                data_summary = {
                    'db_path': db_path,
                    'post_count': post_count,
                    'comment_count': comment_count,
                    'top_posts': [dict(zip(['Post_id', 'Title', 'Author', 'Score', 'Total_comments'], post)) for post in top_posts],
                    'top_comments': [dict(zip(['Comment_id', 'Post_id', 'Author', 'Score', 'Text'], comment)) for comment in top_comments],
                    'most_discussed': [dict(zip(['Post_id', 'Title', 'Author', 'Score', 'Total_comments'], post)) for post in most_discussed]
                }
                
                conn.close()
                return data_summary
                
        raise FileNotFoundError("DuckDB file not found in any of the expected locations")
    except Exception as e:
        logging.error(f"Failed to load DuckDB data: {e}")
        raise

@task
def fetch_conversation_data(data_summary):
    """
    Extract full conversations (post + comments) for analysis
    """
    try:
        db_path = data_summary['db_path']
        conn = duckdb.connect(db_path)
        
        # Get a sample of conversations (up to 10 posts with their comments)
        # This query gets posts and their associated comments, joining on Post_id
        conversations = conn.execute("""
            WITH sample_posts AS (
                SELECT Post_id, Title, Author AS PostAuthor, Text AS PostText, Score AS PostScore, Total_comments
                FROM posts
                ORDER BY Total_comments DESC, Score DESC
                LIMIT 10
            )
            SELECT 
                p.Post_id, p.Title, p.PostAuthor, p.PostText, p.PostScore, p.Total_comments,
                c.Comment_id, c.Author AS CommentAuthor, c.Text AS CommentText, c.Score AS CommentScore
            FROM sample_posts p
            LEFT JOIN comments c ON p.Post_id = c.Post_id
            ORDER BY p.Post_id, c.Score DESC
        """).fetchdf()
        
        # Convert the dataframe to a structured format for easier analysis
        # Organize conversations by post
        structured_conversations = {}
        
        for _, row in conversations.iterrows():
            post_id = row['Post_id']
            
            if post_id not in structured_conversations:
                structured_conversations[post_id] = {
                    'post': {
                        'id': post_id,
                        'title': row['Title'],
                        'author': row['PostAuthor'],
                        'text': row['PostText'] if pd.notna(row['PostText']) else "",
                        'score': int(row['PostScore']),
                        'total_comments': int(row['Total_comments']),
                    },
                    'comments': []
                }
            
            # Skip if there's no comment associated with this row
            if pd.notna(row['Comment_id']):
                structured_conversations[post_id]['comments'].append({
                    'id': row['Comment_id'],
                    'author': row['CommentAuthor'],
                    'text': row['CommentText'] if pd.notna(row['CommentText']) else "",
                    'score': int(row['CommentScore']),
                })
        
        # Close the connection
        conn.close()
        
        # Convert to list for easier processing with Airflow tasks
        conversation_list = []
        for post_id, data in structured_conversations.items():
            conversation = {
                'post_id': post_id,
                'post_title': data['post']['title'],
                'post_author': data['post']['author'],
                'post_text': data['post']['text'],
                'post_score': data['post']['score'],
                'comments_count': len(data['comments']),
                'top_comments': [comment['text'] for comment in data['comments'][:3]]
            }
            conversation_list.append(conversation)
            
        return conversation_list
    except Exception as e:
        logging.error(f"Failed to fetch conversation data: {e}")
        raise

@task.llm(
    model=model,
    result_type=SentimentAnalysis,
    system_prompt="""
    You are an expert sentiment analyzer specialized in social media text.
    
    Analyze the sentiment of the Reddit post and return:
    - sentiment: must be "POSITIVE", "NEGATIVE", or "NEUTRAL" in all uppercase
    - confidence: a value between 0 and 1 indicating your confidence in this analysis
    - reasoning: a brief explanation for your sentiment analysis
    
    Be objective and consider the context of online discussions.
    """
)
def analyze_sentiment(conversation=None):
    if conversation is None:
        raise AirflowSkipException("No conversation provided")
        
    post_text = conversation['post_text'] if conversation['post_text'] else conversation['post_title']
    
    return f"""
    Analyze the sentiment of this Reddit post:
    
    Title: {conversation['post_title']}
    
    Content: {post_text}
    """

@task.llm(
    model=model,
    result_type=TopicAnalysis,
    system_prompt="""
    You are an expert topic analyzer specializing in social media content.
    Given a Reddit post and its comments, identify the main topics being discussed.
    
    Return:
    - main_topic: the primary subject of discussion
    - subtopics: a list of related topics (up to 3)
    - keywords: a list of important terms or phrases from the discussion (up to 5)
    
    Provide your response in a structured format that matches the required fields.
    """
)
def identify_topics(conversation=None):
    if conversation is None:
        raise AirflowSkipException("No conversation provided")
        
    post_text = conversation['post_text'] if conversation['post_text'] else conversation['post_title']
    comments = "\n\n".join(conversation['top_comments'])
    
    return f"""
    Identify topics in this Reddit conversation:
    
    Post Title: {conversation['post_title']}
    
    Post Content: {post_text}
    
    Comments:
    {comments}
    """

@task
def store_sentiment_analysis(data_summary, sentiments, topics, conversations):
    """
    Store sentiment analysis results in DuckDB
    """
    try:
        db_path = data_summary['db_path']
        conn = duckdb.connect(db_path)
        
        # Iterate through each conversation and its analysis results
        for i, (conversation, sentiment, topic) in enumerate(zip(conversations, sentiments, topics)):
            post_id = conversation['post_id']
            
            # Convert subtopics and keywords to comma-separated strings
            subtopics_str = ", ".join(topic['subtopics']) if topic['subtopics'] else ""
            keywords_str = ", ".join(topic['keywords']) if topic['keywords'] else ""
            
            # Ensure sentiment is uppercase
            sentiment_value = sentiment['sentiment'].upper()
            
            # Check if a record for this post already exists
            existing = conn.execute(f"""
                SELECT Post_id FROM post_sentiment 
                WHERE Post_id = '{post_id}'
            """).fetchone()
            
            if existing:
                # Update existing record
                conn.execute(f"""
                    UPDATE post_sentiment
                    SET Sentiment = '{sentiment_value}',
                        Confidence = {sentiment['confidence']},
                        Reasoning = '{sentiment['reasoning'].replace("'", "''")}',
                        Main_Topic = '{topic['main_topic'].replace("'", "''")}',
                        Subtopics = '{subtopics_str.replace("'", "''")}',
                        Keywords = '{keywords_str.replace("'", "''")}',
                        Analysis_Date = CURRENT_TIMESTAMP
                    WHERE Post_id = '{post_id}'
                """)
                logging.info(f"Updated sentiment analysis for post {post_id}: {sentiment_value}")
            else:
                # Insert new record
                conn.execute(f"""
                    INSERT INTO post_sentiment
                    (Post_id, Sentiment, Confidence, Reasoning, Main_Topic, Subtopics, Keywords, Analysis_Date)
                    VALUES (
                        '{post_id}',
                        '{sentiment_value}',
                        {sentiment['confidence']},
                        '{sentiment['reasoning'].replace("'", "''")}',
                        '{topic['main_topic'].replace("'", "''")}',
                        '{subtopics_str.replace("'", "''")}',
                        '{keywords_str.replace("'", "''")}',
                        CURRENT_TIMESTAMP
                    )
                """)
                logging.info(f"Inserted sentiment analysis for post {post_id}")
        
        # Log summary information
        analysis_count = conn.execute("SELECT COUNT(*) FROM post_sentiment").fetchone()[0]
        logging.info(f"Total posts with sentiment analysis: {analysis_count}")
        
        # Get sentiment distribution for logging
        sentiment_counts = conn.execute("""
            SELECT Sentiment, COUNT(*) as Count
            FROM post_sentiment
            GROUP BY Sentiment
        """).fetchdf()
        logging.info(f"Sentiment distribution: {sentiment_counts.to_dict()}")
        
        conn.close()
        return {
            'posts_analyzed': len(conversations),
            'sentiment_distribution': sentiment_counts.to_dict() if not sentiment_counts.empty else {}
        }
    except Exception as e:
        logging.error(f"Failed to store sentiment analysis: {e}")
        raise

@task
def generate_sentiment_summary(data_summary, analysis_results):
    """
    Generate a summary of the sentiment analysis results in the database
    """
    try:
        db_path = data_summary['db_path']
        conn = duckdb.connect(db_path)
        
        # Get sentiment distribution
        sentiment_counts = conn.execute("""
            SELECT Sentiment, COUNT(*) as Count
            FROM post_sentiment
            GROUP BY Sentiment
            ORDER BY Count DESC
        """).fetchdf()
        
        # Get top topics
        topics = conn.execute("""
            SELECT Main_Topic, COUNT(*) as Count
            FROM post_sentiment
            GROUP BY Main_Topic
            ORDER BY Count DESC
            LIMIT 5
        """).fetchdf()
        
        # Get most confident sentiments
        top_sentiments = conn.execute("""
            SELECT Post_id, Sentiment, Confidence, Reasoning
            FROM post_sentiment
            ORDER BY Confidence DESC
            LIMIT 5
        """).fetchdf()
        
        conn.close()
        
        return {
            'sentiment_distribution': sentiment_counts.to_dict(),
            'top_topics': topics.to_dict(),
            'top_sentiments': top_sentiments.to_dict(),
            'analysis_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logging.error(f"Failed to generate sentiment summary: {e}")
        raise

@task
def query_sentiment_data(data_summary):
    """
    Query the sentiment analysis table to display results
    """
    try:
        db_path = data_summary['db_path']
        conn = duckdb.connect(db_path)
        
        # Join posts with sentiment analysis
        results = conn.execute("""
            SELECT 
                p.Post_id,
                p.Title,
                p.Author,
                p.Score,
                p.Total_comments,
                s.Sentiment,
                s.Confidence,
                s.Main_Topic,
                s.Subtopics,
                s.Analysis_Date
            FROM posts p
            JOIN post_sentiment s ON p.Post_id = s.Post_id
            ORDER BY s.Confidence DESC
        """).fetchdf()
        
        logging.info(f"Retrieved {len(results)} posts with sentiment analysis")
        
        # Display the first few rows in the log
        if not results.empty:
            sample = results.head(5)
            for _, row in sample.iterrows():
                logging.info(f"Post: {row['Title']} | Sentiment: {row['Sentiment']} | Confidence: {row['Confidence']:.2f} | Topic: {row['Main_Topic']}")
        
        conn.close()
        return results.to_dict() if not results.empty else {}
    except Exception as e:
        logging.error(f"Failed to query sentiment data: {e}")
        return {}

@task.llm(
    model=model,
    result_type=RedditInsights,
    system_prompt="""
    You are an advanced social media analyst specializing in Reddit communities.
    Based on the provided data about posts, comments, sentiments, and topics,
    generate a comprehensive insights report about this Reddit community.
    
    Return:
    - summary: a concise summary of the community discussions
    - sentiment_trend: overall sentiment pattern observed
    - key_topics: list of the most important topics in the community
    - recommendations: list of suggestions for engaging with this community
    """
)
def generate_insights_report(data_summary=None, sentiment_results=None, topic_results=None):
    if data_summary is None or sentiment_results is None or topic_results is None:
        raise AirflowSkipException("Missing required analysis data")
    
    # Convert data to strings for the prompt
    post_count = data_summary['post_count']
    comment_count = data_summary['comment_count']
    top_posts = "\n".join([f"- {post['Title']} (Score: {post['Score']})" for post in data_summary['top_posts'][:3]])
    
    # Prepare sentiment summary
    sentiment_counts = {
        "positive": sum(1 for result in sentiment_results if result['sentiment'] == "positive"),
        "negative": sum(1 for result in sentiment_results if result['sentiment'] == "negative"),
        "neutral": sum(1 for result in sentiment_results if result['sentiment'] == "neutral")
    }
    sentiment_summary = ", ".join([f"{key}: {value}" for key, value in sentiment_counts.items()])
    
    # Prepare topic summary
    all_topics = [topic['main_topic'] for topic in topic_results]
    all_subtopics = [item for sublist in [topic['subtopics'] for topic in topic_results] for item in sublist]
    topic_summary = ", ".join(set(all_topics[:5]))
    
    return f"""
    Generate insights for this Reddit community:
    
    Statistics:
    - Total Posts: {post_count}
    - Total Comments: {comment_count}
    
    Top Posts:
    {top_posts}
    
    Sentiment Distribution:
    {sentiment_summary}
    
    Main Topics:
    {topic_summary}
    """



@dag(
    dag_id='reddit_analyzer_dag',
    default_args=default_args,
    description='DAG for analyzing Reddit data from DuckDB and storing sentiment in database',
    # schedule_interval='@daily',
    catchup=False,
    tags=['reddit_analysis', 'ollama', 'ai'],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    max_active_tasks=3,
)
def reddit_analyzer():
    # Task flow
    dependency_check = check_ai_dependencies()
    data = load_duckdb_data()
    
    # Initialize sentiment table if it doesn't exist
    init_table = initialize_sentiment_table(data)
    
    # Get conversations to analyze
    conversations = fetch_conversation_data(data)
    
    # Perform sentiment analysis on each conversation
    sentiments = analyze_sentiment.expand(conversation=conversations)
    
    # Identify topics in each conversation
    topics = identify_topics.expand(conversation=conversations)
    
    # Store the results in DuckDB
    analysis_results = store_sentiment_analysis(
        data_summary=data,
        sentiments=sentiments,
        topics=topics,
        conversations=conversations
    )
    
    # Generate summary of sentiment analysis
    sentiment_summary = generate_sentiment_summary(
        data_summary=data,
        analysis_results=analysis_results
    )
    
    # Query and display the sentiment data
    query_results = query_sentiment_data(data)

# Create the DAG instance
reddit_analyzer_dag = reddit_analyzer()

# For testing the DAG locally
if __name__ == "__main__":
    reddit_analyzer_dag.test()