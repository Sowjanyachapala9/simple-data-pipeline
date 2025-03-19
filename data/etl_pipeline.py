import pandas as pd
import sqlite3
import os

# Step 1: Extract
def extract_data(file_path):
    print("Extracting data...")
    df = pd.read_csv(file_path)
    return df

# Step 2: Transform
def transform_data(df):
    print("Transforming data...")
    # Example transformation: filter out salaries less than 60000
    df_filtered = df[df['salary'] >= 60000]
    return df_filtered

# Step 3: Load
def load_data(df, db_name, table_name):
    print("Loading data into database...")
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def run_etl():
    file_path = os.path.join('data', 'source_data.csv')
    db_name = 'processed_data.db'
    table_name = 'employees'

    # Run ETL steps
    data = extract_data(file_path)
    transformed_data = transform_data(data)
    load_data(transformed_data, db_name, table_name)
    print("ETL process completed!")

if __name__ == "__main__":
    run_etl()

