import os
import argparse
import pandas as pd
import json
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def load_mysql_config(config_path):
    print("📄 Loading MySQL config from", config_path)
    with open(config_path, 'r') as f:
        return json.load(f)

def create_sqlalchemy_engine(config):
    password_encoded = quote_plus(config["password"])
    url = f"mysql+pymysql://{config['username']}:{password_encoded}@{config['host']}:{config['port']}/{config['database']}"
    print(f"🔗 Engine URL: {url}")
    print("🔌 Connecting to MySQL using SQLAlchemy...")
    engine = create_engine(url, echo=True)  # echo=True helps debug
    return engine

def upload_csv_to_mysql(engine, csv_path, table_name):
    print(f"📤 Uploading {csv_path} to table '{table_name}'...")
    df = pd.read_csv(csv_path)

    print(f"📊 Loaded CSV with {len(df)} rows and {len(df.columns)} columns.")
    print("🧾 DataFrame Columns:", list(df.columns))

    df.columns = [col.strip().replace(" ", "_") for col in df.columns]
    print("🧾 Cleaned Columns:", list(df.columns))

    print("🔍 Preview of data being uploaded:")
    print(df.head(3))

    df.to_sql(table_name, con=engine, if_exists='replace', index=False, method='multi')
    print("✅ Data uploaded successfully!")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_dir", required=True, help="Directory containing CSV files")
    parser.add_argument("--mysql_details", required=True, help="Path to MySQL config JSON file")
    parser.add_argument("--destination_table", required=True, help="Table name to write into")
    args = parser.parse_args()

    config = load_mysql_config(args.mysql_details)
    engine = create_sqlalchemy_engine(config)

    for file in os.listdir(args.source_dir):
        if file.endswith(".csv"):
            full_path = os.path.join(args.source_dir, file)
            upload_csv_to_mysql(engine, full_path, args.destination_table)

if __name__ == "__main__":
    main()
