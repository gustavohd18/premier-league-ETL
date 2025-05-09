# Premier League ETL

This project performs the extraction, transformation, and loading (ETL) of data from the Premier League matches, sourced from the [WorldFootball.net](https://www.worldfootball.net) website. The script is capable of extracting match data, transforming it, and saving it either as a local CSV file or uploading it to an Amazon S3 bucket.

## Functionality

The script follows the typical ETL process:

1. **Extract**: Fetches the match data from a webpage containing the Premier League matches.
2. **Transform**: Processes the data, calculating goal differences, total goals, and the winner of each match.
3. **Load**: Saves the data in CSV format, either locally or to an S3 bucket.

### Code Structure

1. **Extraction (Extract)**: 
   - Collects the match data from the Premier League matches webpage.
   - Uses the `requests` library to make HTTP requests.
   - Uses `BeautifulSoup` to parse and extract the data from the HTML table.

2. **Transformation (Transform)**:
   - Adds columns for **goal difference**, **total goals**, and **winner**.

3. **Loading (Load)**:
   - Allows saving the data as a CSV file locally or directly uploading it to S3.
   - Uses `boto3` to interact with S3 and save the CSV file.

## Installation

### Run
    python etl.py --s3 to use s3 storage
    python etl.py to save local csv

### Dependencies

Before running the script, install the necessary dependencies:

```bash
pip install requests beautifulsoup4 pandas boto3



