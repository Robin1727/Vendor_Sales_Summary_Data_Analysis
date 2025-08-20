import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# ---------------- LOGGING SETUP ----------------
os.makedirs("logs", exist_ok=True)  # Create logs folder if not exists
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ---------------- DATABASE SETUP ----------------
engine = create_engine('sqlite:///inventory.db')

# ---------------- FUNCTIONS ----------------
def ingest_db(df, table_name, engine):
    """Ingests dataframe into a database table."""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data(data_folder):
    """Loads CSVs from folder and ingests into DB."""
    start = time.time()
    
    for file in os.listdir(data_folder):
        if file.lower().endswith('.csv'):
            file_path = os.path.join(data_folder, file)
            logging.info(f"Processing file: {file_path}")
            
            # Load in chunks for memory efficiency
            chunk_size = 50000  # 50k rows at a time
            first_chunk = True
            total_rows = 0
            file_start = time.time()
            
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                if first_chunk:
                    chunk.to_sql(file[:-4], con=engine, if_exists='replace', index=False)
                    first_chunk = False
                else:
                    chunk.to_sql(file[:-4], con=engine, if_exists='append', index=False)
                total_rows += len(chunk)
            
            file_end = time.time()
            logging.info(f"Completed {file} | Rows: {total_rows:,} | Time: {(file_end - file_start):.2f} sec")
    
    end = time.time()
    logging.info("-------------- Ingestion Complete --------------")
    logging.info(f"Total Time Taken: {(end - start) / 60:.2f} minutes")
    
 # ---------------- MAIN ----------------
if __name__ == '__main__':
    data_folder = r"C:\Users\ROBIN SINGH\OneDrive\Desktop\Inventory\data"
    load_raw_data(data_folder)
    
