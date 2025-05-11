"""
## Reddit Data Scraping DAG

This DAG scrapes posts and comments from the "eb_1a" subreddit using the Reddit API
and saves the data to a CSV file. The DAG implements checkpointing to save data every
10 posts to prevent data loss in case of failure.

The DAG saves data to both:
1. Inside the container: /usr/local/airflow/data/ (accessible only from within the container)
2. To the mounted volume: /usr/local/airflow/dags/data/ (synced to your local machine)

The DAG uses PRAW (Python Reddit API Wrapper) to interact with the Reddit API
and includes proper rate limiting to avoid API throttling.

Dependencies:
- praw
- pandas
- python-dotenv
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
    'schedule_interval': '@daily',
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
    required_packages = ['praw', 'pandas', 'dotenv']
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
        
        POSTS_LIMIT = 10_000  # Set the limit for the number of posts to scrape
        
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

# Set task dependencies
check_dependencies_task >> load_config_task >> scrape_reddit_data_task >> process_data_summary_task
