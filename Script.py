import pandas as pd
import sqlite3



### Function for Data Cleaning and Transformation
def clean_and_transform_data(df1, df2):
    # Combine the two DataFrames into a single DataFrame
    df = pd.concat([df1, df2])
    
    # Remove unnecessary columns
    df = df[['album','name', 'artists', 'duration_ms', 'popularity', 'explicit', 'preview_url']]
    
    # Convert duration_ms to minutes:seconds

    def ms_to_min_sec(ms):
        minutes, seconds = divmod(ms / 1000, 60)
        return f"{int(minutes)}:{int(seconds)}"

    df['duration_ms'] = df['duration_ms'].apply(lambda x: ms_to_min_sec(x))
    
     # Remove duplicate songs
    df = df.drop_duplicates()
    
     # Filter out songs with null preview_url
    df = df[df['preview_url'].notnull()]
    
     # Rename and reorder columns
    df = df.rename(columns={'name': 'song', 'duration_ms': 'duration'})
    
    
    # Transforming the `artists` columns
    df['artists'] = df['artists'].str.replace("[\['\]']", "",  regex=True)
    
    
    return df
    
### Function for load the cleaned and transformed data into a table in a database
def load_data_into_database(df):
    # Connect to the database
    conn = sqlite3.connect('mydatabase.db')

    # Write the DataFrame to a table in the database
    df.to_sql('playlists', conn, if_exists='replace')

    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()
    
### Pipeline for data cleaning, transformation and loading into an SQL table    
if __name__ == '__main__':
    # Read in the first CSV
    df1 = pd.read_csv('playlist1.csv')

    # Read in the second CSV
    df2 = pd.read_csv('playlist2.csv')

    # Clean and transform the data
    df = clean_and_transform_data(df1, df2)

    # Load the data into the database
    load_data_into_database(df)