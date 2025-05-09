import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import boto3
from datetime import datetime
from io import StringIO

# 1. Extract
def extract_matches(url):
    print("Extracting data...")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'standard_tabelle'})
    rows = table.find_all('tr')[1:]

    data = []
    last_date = None

    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 6:
            continue

        raw_date = cols[0].text.strip()

        if any(char.isdigit() for char in raw_date) and '/' in raw_date:
            last_date = raw_date
        elif last_date is None:
            continue  

        date = last_date
        home = cols[2].text.strip()
        away = cols[4].text.strip()
        result = cols[5].text.strip()

        if ':' not in result:
            continue

        clean_result = result.split('(')[0].strip()
        try:
            home_score, away_score = map(int, clean_result.split(':'))
        except ValueError:
            continue 

        data.append({
            'date': date,
            'home_team': home,
            'away_team': away,
            'home_score': home_score,
            'away_score': away_score
        })

    return pd.DataFrame(data)

# 2. Transform
def transform_matches(df):
    print("Transform data...")
    df['goal_difference'] = df['home_score'] - df['away_score']
    df['total_goals'] = df['home_score'] + df['away_score']
    df['winner'] = df.apply(lambda row: row['home_team'] if row['home_score'] > row['away_score']
                            else row['away_team'] if row['away_score'] > row['home_score']
                            else 'Draw', axis=1)
    return df

# 3. Save functions
def save_to_local_csv(df, filename):
    df.to_csv(filename, index=False)
    print(f'The date was saved: {filename}')

def save_to_s3(df, bucket_name, s3_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f'The date was sent to s3://{bucket_name}/{s3_key}')

# Run ETL
def run_etl(save_to_s3_flag=False):
    url = 'https://www.worldfootball.net/all_matches/eng-premier-league-2023-2024/'
    df = extract_matches(url)
    df_transformed = transform_matches(df)

    if save_to_s3_flag:
        bucket_name = 'your-s3-bucket-name'  # Replace with your S3 bucket name
        current_date = datetime.now().strftime('%Y-%m-%d')
        s3_key = f'dados/premier_league_matches_{current_date}.csv'
        save_to_s3(df_transformed, bucket_name, s3_key)
    else:
        save_to_local_csv(df_transformed, 'matches.csv')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ETL de partidas da Premier League')
    parser.add_argument('--s3', action='store_true', help='Salvar no S3 em vez de localmente')
    args = parser.parse_args()

    run_etl(save_to_s3_flag=args.s3)
