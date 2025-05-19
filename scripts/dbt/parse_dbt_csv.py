import duckdb
import pandas as pd
import os

# Define the source CSV file and target DuckDB database
source_csv = "./data/eb1a_threads_data.csv"
target_db = "eb1a_threads_data.duckdb"

# Check if the CSV file exists
if not os.path.exists(source_csv):
    print(f"CSV file not found at {source_csv}")
    print("Current working directory:", os.getcwd())
    # Try alternate path
    alternate_path = "../data/eb1a_threads_data.csv"
    if os.path.exists(alternate_path):
        source_csv = alternate_path
        print(f"Found CSV at alternate path: {alternate_path}")
    else:
        print(f"CSV file not found at alternate path either: {alternate_path}")
        exit(1)

print(f"Loading data from {source_csv}")

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

print(f"Successfully inserted {post_count} posts and {comment_count} comments into DuckDB.")

# Example queries to demonstrate the relationship between posts and comments
print("\nExample of posts with their comments:")
result = conn.execute("""
SELECT p.Post_id, p.Title, p.Author as PostAuthor, 
       c.Comment_id, c.Author as CommentAuthor, c.Text as CommentText
FROM posts p
JOIN comments c ON p.Post_id = c.Post_id
LIMIT 5
""").fetchall()

if result:
    for row in result:
        print(f"Post {row[0]}: '{row[1]}' by {row[2]}")
        print(f"   Comment {row[3]}: by {row[4]}")
        comment_text = row[5]
        if comment_text and len(comment_text) > 100:
            comment_text = comment_text[:100] + "..."
        print(f"   Text: {comment_text}")
        print("")
else:
    print("No joined post-comment data found.")

# Close the connection
conn.close()

print("ETL process completed. Data has been loaded into DuckDB.")
