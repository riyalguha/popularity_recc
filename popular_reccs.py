from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
import psycopg2 

#db_url = "host=arjuna.db.elephantsql.com user=zpzenhka password=va0hKEJBMlferhvZOvqgll5uv1u-VT90 dbname=zpzenhka port=5432 sslmode=disable"
rows = None


def fetch_from_db():
    db_params = {
    "host": "arjuna.db.elephantsql.com",
    "user": "zpzenhka",
    "password": "va0hKEJBMlferhvZOvqgll5uv1u-VT90",
    "dbname": "zpzenhka",
    "port": 5432,
    "sslmode": "disable",
    }
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    try:
        # Execute a query to fetch everything from a table (replace 'your_table_name' with the actual table name)
        query = "SELECT * FROM news_posts;"
        cursor.execute(query)

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Define column names based on your database schema
        column_names = ["id", "created_at", "updated_at", "some_column", "Title", "Content", "ShortDescription", "User", "Likes"]

        # Create a DataFrame using fetched rows and column names
        df = pd.DataFrame(rows, columns=column_names)
        return df

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
        


app = FastAPI()

origins = [
    "http://localhost:3000",  # Replace this with the origin of your frontend
    # Add more origins as needed
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_popular_articles():
    try:
        df = fetch_from_db()
        
        # Calculate popularity score
        df['popularity_score'] = df['Likes'] * 0.4
        
        # Get top 5 popular articles
        top_articles = df.sort_values('popularity_score', ascending=False).head(5)
        
        # Drop the popularity_score column
        top_articles = top_articles[['id','Title','Content','ShortDescription','User','Likes']]
        
        # Drop the popularity_score column
        
        # Convert DataFrame to JSON
        popular_json = top_articles.to_json(orient='records')
        
        return popular_json
    except requests.RequestException as e:
        # Handle request exceptions
        raise HTTPException(status_code=500, detail=f"Failed to fetch data from backend API: {str(e)}")


    
@app.get("/")
async def popular_articles_handler():
    try:
        popular_json = get_popular_articles()
        return JSONResponse(content=popular_json)
    except HTTPException as e:
        # Propagate HTTPExceptions
        raise e
    except Exception as e:
        # Handle other exceptions
        raise HTTPException(status_code=500, detail=str(e))