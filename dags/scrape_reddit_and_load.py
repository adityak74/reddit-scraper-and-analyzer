"""
## Reddit Data Scraping DAG

This DAG scrapes posts and comments from the "eb_1a" subreddit using the Reddit API
and saves the data to a CSV file. The DAG implements checkpointing to save data every
10 posts to prevent data loss in case of failure.

The DAG saves data to both:
1. Inside the container: /usr/local/airflow/data/ (accessible only from within the container)
2. To the mounted volume: /usr/local/airflow/dags/data/ (synced to your local machine)

The data is also processed into a DuckDB database for easier querying and analysis.

The DAG uses PRAW (Python Reddit API Wrapper) to interact with the Reddit API
and includes proper rate limiting to avoid API throttling.

Dependencies:
- praw
- pandas
- python-dotenv
- duckdb
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os

# Define the default_args for the DAG
default_args = {
    'owner': 'Astro',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'reddit_scraper_dag',
    default_args=default_args,
    description='DAG for scraping Reddit data from eb_1a subreddit',
    # schedule_interval='@daily',
    catchup=False,
    tags=['reddit_scraping'],
)

# Define functions for each task
def check_dependencies(**kwargs):
    """
    Check if all required dependencies are installed
    """
    required_packages = ['praw', 'pandas', 'dotenv', 'duckdb']
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
    
    return "All dependencies are installed"

def load_config(**kwargs):
    """
    Load environment variables and create configuration for Reddit API
    """
    try:
        # Import required packages
        from dotenv import dotenv_values
        from models import Config
        
        # Load environment variables from .env file in the Airflow home directory
        # In Airflow, the working directory is different, so we need to adjust the path
        airflow_home = os.environ.get('AIRFLOW_HOME', '.')
        env_path = os.path.join(airflow_home, '.env')
        
        if os.path.exists(env_path):
            config_vars = dotenv_values(env_path)
        else:
            # Fall back to environment variables if .env doesn't exist
            config_vars = {
                "REDDIT_CLIENT_ID": os.environ.get("REDDIT_CLIENT_ID"),
                "REDDIT_CLIENT_SECRET": os.environ.get("REDDIT_CLIENT_SECRET"),
                "REDDIT_USER_AGENT": os.environ.get("REDDIT_USER_AGENT"),
                "REDDIT_RATE_LIMIT": os.environ.get("REDDIT_RATE_LIMIT", "60")
            }
        
        # Push the config to XCom for the next task
        kwargs['ti'].xcom_push(key='config_vars', value=config_vars)
        return "Config loaded successfully"
    except ImportError as e:
        logging.error(f"Failed to import required module: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        raise


def scrape_reddit_data(**kwargs):
    """
    Connect to Reddit API and scrape posts and comments from the eb_1a subreddit
    """
    try:
        # Import required packages
        import praw
        import pandas as pd
        import time
        import shutil
        from models import Config
        
        # Set up logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")
        
        # Get config from XCom
        ti = kwargs['ti']
        config_vars = ti.xcom_pull(key='config_vars', task_ids='load_config_task')
        
        # Convert config_vars to Config object
        config = Config(**config_vars)
        
        POSTS_LIMIT = 10  # Set the limit for the number of posts to scrape
        
        # Initialize Reddit API client
        reddit = praw.Reddit(
            client_id=config.REDDIT_CLIENT_ID,
            client_secret=config.REDDIT_CLIENT_SECRET,
            user_agent=config.REDDIT_USER_AGENT,
            ratelimit_seconds=config.REDDIT_RATE_LIMIT,
        )
        
        # Specify the subreddit to scrape
        subreddit = reddit.subreddit("eb_1a")
        
        # Display the name of the Subreddit
        logging.info("Display Name: %s", subreddit.display_name)
        logging.info("Title: %s", subreddit.title)
        logging.info("Description: %s", subreddit.description)
        
        # Prepare data storage
        data = []
        
        # Initialize a counter for checkpointing
        checkpoint_counter = 0
        
        # Define output paths
        airflow_home = os.environ.get('AIRFLOW_HOME', '.')
        
        # Path inside container (not accessible from host)
        container_output_path = os.path.join(airflow_home, 'data')
        container_output_file = os.path.join(container_output_path, 'eb1a_threads_data.csv')
        
        # Path in mounted volume (accessible from host)
        host_output_path = os.path.join(airflow_home, 'dags', 'data')
        host_output_file = os.path.join(host_output_path, 'eb1a_threads_data.csv')
        
        # Ensure data directories exist
        os.makedirs(container_output_path, exist_ok=True)
        os.makedirs(host_output_path, exist_ok=True)
        
        logging.info(f"Data will be saved to container path: {container_output_file}")
        logging.info(f"Data will be saved to host-accessible path: {host_output_file}")
        
        logging.info("Scraping posts and comments from subreddit: %s", subreddit.display_name)
        
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
                df = pd.DataFrame(data)
                
                # Save to container path
                file_exists = os.path.isfile(container_output_file)
                df.to_csv(container_output_file, mode='a', index=False, header=not file_exists)
                
                # Save to host-accessible path
                file_exists = os.path.isfile(host_output_file)
                df.to_csv(host_output_file, mode='a', index=False, header=not file_exists)
        
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
                            "Timestamp": comment.created_utc,
                            "Text": comment.body,
                            "Score": comment.score,
                            "Total_comments": 0,  # Comments don't have this attribute
                            "Post_URL": None,  # Comments don't have this attribute
                        }
                    )
        
            # Sleep for 2 seconds before next post
            logging.info("Sleeping for 2 seconds before scraping the next post.")
            time.sleep(2)
        
        # Final save of the DataFrame to CSV files
        logging.info("Saving final data to CSV files.")
        eb1a_threads_data = pd.DataFrame(data)
        
        # Save to container path
        eb1a_threads_data.to_csv(container_output_file, index=False)
        logging.info(f"Data saved to container path: {container_output_file}")
        
        # Save to host-accessible path
        eb1a_threads_data.to_csv(host_output_file, index=False)
        logging.info(f"Data saved to host-accessible path: {host_output_file}")
        
        # Additional copy to project root directory
        project_root_path = os.path.join(airflow_home, 'dags', '..')
        project_root_file = os.path.join(project_root_path, 'eb1a_threads_data.csv')
        try:
            eb1a_threads_data.to_csv(project_root_file, index=False)
            logging.info(f"Data also saved to project root: {project_root_file}")
        except Exception as e:
            logging.warning(f"Could not save to project root: {e}")
            
        # Store both paths in XCom for the next task
        output_files = {
            'container_file': container_output_file,
            'host_file': host_output_file
        }
        ti.xcom_push(key='output_files', value=output_files)
        
        # For backward compatibility
        ti.xcom_push(key='output_file', value=container_output_file)
        
        return output_files
    except ImportError as e:
        logging.error(f"Failed to import required module: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to scrape Reddit data: {e}")
        raise


def process_data_summary(**kwargs):
    """
    Process the scraped data and provide a summary
    """
    try:
        import pandas as pd
        
        ti = kwargs['ti']
        output_files = ti.xcom_pull(key='output_files', task_ids='scrape_reddit_data_task')
        
        # Get both container and host file paths
        container_file = output_files.get('container_file')
        host_file = output_files.get('host_file')
        
        # Check container file first (should always exist)
        data_file = container_file if os.path.exists(container_file) else host_file
        
        if os.path.exists(data_file):
            df = pd.read_csv(data_file)
            post_count = df[df['Type'] == 'Post'].shape[0]
            comment_count = df[df['Type'] == 'Comment'].shape[0]
            
            logging.info("Data Summary:")
            logging.info(f"Total posts scraped: {post_count}")
            logging.info(f"Total comments scraped: {comment_count}")
            logging.info(f"Total data entries: {df.shape[0]}")
            logging.info(f"Data saved to container path: {container_file}")
            logging.info(f"Data saved to host-accessible path: {host_file}")
            logging.info(f"You can access the data file on your local machine at: {os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'eb1a_threads_data.csv')}")
            
            return {
                "post_count": post_count,
                "comment_count": comment_count,
                "total_entries": df.shape[0],
                "container_file": container_file,
                "host_file": host_file
            }
        else:
            logging.warning(f"Data file not found at: {data_file}")
            return None
    except ImportError as e:
        logging.error(f"Failed to import required module: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to process data summary: {e}")
        raise

def process_csv_to_duckdb(**kwargs):
    """
    Process the CSV data into a DuckDB database for easier querying and analysis.
    Based on the existing parse_dbt_csv.py script.
    """
    try:
        import duckdb
        import pandas as pd
        import os
        
        # Get file paths from XCom
        ti = kwargs['ti']
        output_files = ti.xcom_pull(key='output_files', task_ids='scrape_reddit_data_task')
        
        # Set paths for source and target files
        airflow_home = os.environ.get('AIRFLOW_HOME', '.')
        
        # Use the host-accessible path as the source CSV
        source_csv = output_files.get('host_file')
        
        # Set target DuckDB paths (both in container and host-accessible)
        container_db_path = os.path.join(airflow_home, 'data', 'eb1a_threads_data.duckdb')
        host_db_path = os.path.join(airflow_home, 'dags', 'data', 'eb1a_threads_data.duckdb')
        project_root_db_path = os.path.join(airflow_home, 'eb1a_threads_data.duckdb')
        
        # Use the first available path
        if os.path.exists(source_csv):
            logging.info(f"Loading data from {source_csv}")
            target_db = host_db_path  # Prefer the host-accessible path
        else:
            # Try container path as fallback
            container_csv = output_files.get('container_file')
            if os.path.exists(container_csv):
                source_csv = container_csv
                target_db = container_db_path
                logging.info(f"Using container CSV path: {source_csv}")
            else:
                error_msg = f"CSV file not found at any location"
                logging.error(error_msg)
                raise FileNotFoundError(error_msg)
        
        logging.info(f"Source CSV: {source_csv}")
        logging.info(f"Target DB: {target_db}")
        
        # Load the CSV file into a DataFrame
        data = pd.read_csv(source_csv)
        
        # Create a connection to the DuckDB database
        conn = duckdb.connect(target_db)
        
        # Drop tables if they exist to start fresh
        conn.execute("DROP TABLE IF EXISTS posts;")
        conn.execute("DROP TABLE IF EXISTS comments;")
        
        # Separate posts and comments from the DataFrame
        posts = data[data['Type'] == 'Post'].copy()  # Use .copy() to avoid SettingWithCopyWarning
        post_data = posts[['Post_id', 'Title', 'Author', 'Timestamp', 'Text', 'Score', 'Total_comments', 'Post_URL']]
        
        comments = data[data['Type'] == 'Comment'].copy()
        comments = comments.reset_index(drop=True)
        comments['Comment_id'] = range(1, len(comments) + 1)
        comment_data = comments[['Comment_id', 'Post_id', 'Author', 'Timestamp', 'Text', 'Score']]
        
        # Convert types to ensure compatibility
        post_data.loc[:, 'Post_id'] = post_data['Post_id'].astype(str)
        post_data.loc[:, 'Timestamp'] = pd.to_datetime(pd.to_numeric(post_data['Timestamp'], errors='coerce'), unit='s', errors='coerce')
        
        comment_data.loc[:, 'Post_id'] = comment_data['Post_id'].astype(str)
        comment_data.loc[:, 'Timestamp'] = pd.to_datetime(comment_data['Timestamp'], errors='coerce')
        
        # Register the DataFrames as DuckDB views
        conn.register("post_data_view", post_data)
        conn.register("comment_data_view", comment_data)
        
        # Create tables directly from the views
        conn.execute("CREATE TABLE posts AS SELECT * FROM post_data_view")
        conn.execute("CREATE TABLE comments AS SELECT * FROM comment_data_view")
        
        # Execute a sample query to verify the data was inserted
        post_count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        comment_count = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
        
        logging.info(f"Successfully inserted {post_count} posts and {comment_count} comments into DuckDB.")
        
        # Example queries to demonstrate the relationship between posts and comments
        logging.info("\nExample of posts with their comments:")
        result = conn.execute("""
        SELECT p.Post_id, p.Title, p.Author as PostAuthor, 
               c.Comment_id, c.Author as CommentAuthor, c.Text as CommentText
        FROM posts p
        JOIN comments c ON p.Post_id = c.Post_id
        LIMIT 5
        """).fetchall()
        
        if result:
            for row in result:
                logging.info(f"Post {row[0]}: '{row[1]}' by {row[2]}")
                logging.info(f"   Comment {row[3]}: by {row[4]}")
                comment_text = row[5]
                if comment_text and len(str(comment_text)) > 100:
                    comment_text = str(comment_text)[:100] + "..."
                logging.info(f"   Text: {comment_text}")
        else:
            logging.info("No joined post-comment data found.")
        
        # Close the connection
        conn.close()
        
        # Also save to project root and other locations for accessibility
        try:
            # Copy the DuckDB file to the project root for easier access
            import shutil
            shutil.copy2(target_db, project_root_db_path)
            logging.info(f"DuckDB file also copied to project root: {project_root_db_path}")
            
            # If we used the host path, also copy to container path for completeness
            if target_db == host_db_path:
                os.makedirs(os.path.dirname(container_db_path), exist_ok=True)
                shutil.copy2(target_db, container_db_path)
                logging.info(f"DuckDB file also copied to container path: {container_db_path}")
        except Exception as e:
            logging.warning(f"Could not copy DuckDB file to additional locations: {e}")
        
        logging.info("ETL process completed. Data has been loaded into DuckDB.")
        
        # Return the paths for reference
        return {
            "duckdb_path": target_db,
            "root_duckdb_path": project_root_db_path,
            "post_count": post_count,
            "comment_count": comment_count
        }
    except ImportError as e:
        logging.error(f"Failed to import required module for DuckDB processing: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to process CSV to DuckDB: {e}")
        raise

# Create the tasks
check_dependencies_task = PythonOperator(
    task_id='check_dependencies_task',
    python_callable=check_dependencies,
    # provide_context=True,
    dag=dag,
)

load_config_task = PythonOperator(
    task_id='load_config_task',
    python_callable=load_config,
    # provide_context=True,
    dag=dag,
)

scrape_reddit_data_task = PythonOperator(
    task_id='scrape_reddit_data_task',
    python_callable=scrape_reddit_data,
    # provide_context=True,
    dag=dag,
)

process_data_summary_task = PythonOperator(
    task_id='process_data_summary_task',
    python_callable=process_data_summary,
    # provide_context=True,
    dag=dag,
)

# Add new task to process CSV to DuckDB
process_csv_to_duckdb_task = PythonOperator(
    task_id='process_csv_to_duckdb_task',
    python_callable=process_csv_to_duckdb,
    dag=dag,
)

# Set task dependencies
check_dependencies_task >> load_config_task >> scrape_reddit_data_task >> process_data_summary_task >> process_csv_to_duckdb_task
