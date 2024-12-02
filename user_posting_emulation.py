import json
import random
from datetime import datetime
from time import sleep

import requests
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class AWSDBConnector:
    """
    A class to manage connections to an AWS RDS database.

    Attributes:
        HOST (str): Database host URL.
        USER (str): Database username.
        PASSWORD (str): Database password.
        DATABASE (str): Database name.
        PORT (int): Database port.
    """

    def __init__(self, creds_file: str):
        """
        Initialize the database connector by loading credentials from a YAML file.

        Args:
            creds_file (str): Path to the YAML file containing database credentials.
        """
        with open(creds_file, "r", encoding="utf-8") as file:
            creds = yaml.safe_load(file)

            if not isinstance(creds, dict):
                raise ValueError("Invalid YAML format in credentials file.")

            self.HOST = creds["HOST"]
            self.USER = creds["USER"]
            self.PASSWORD = creds["PASSWORD"]
            self.DATABASE = creds["DATABASE"]
            self.PORT = creds["PORT"]

    def create_db_connector(self) -> Engine:
        """
        Create a database connection engine.

        Returns:
            Engine: SQLAlchemy database engine.
        """
        return create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )


def fetch_random_row(engine: Engine, table_name: str, random_row: int) -> dict:
    """
    Fetch a random row from a given table.

    Args:
        engine (Engine): SQLAlchemy database engine.
        table_name (str): Name of the table to query.
        random_row (int): Random row offset to fetch.

    Returns:
        dict: A dictionary representing the selected row.
    """
    query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    with engine.connect() as connection:
        result = connection.execute(query)
        for row in result:
            return dict(row._mapping)
    return {}


def run_infinite_post_data_loop(creds_file: str, api_url: str, tables_to_topics):
    """
    Continuously fetch and send random rows from the database tables to Kafka.

    Args:
        creds_file (str): Path to the YAML file containing database credentials.
        api_url (str): Base URL of the API to post data.
    """
    db_connector = AWSDBConnector(creds_file)
    engine = db_connector.create_db_connector()

    while True:
        sleep(random.randint(0, 2))  # Introduce random delay
        random_row = random.randint(0, 11000)  # Fetch a random row offset

        for table, topic in tables_to_topics.items():
            result = fetch_random_row(engine, table, random_row)
            if result:
                send_data_to_kafka(api_url, topic, result)
            else:
                print(f"No data found in {table} at row {random_row}")


def convert_datetime_fields(data: dict):
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()  # Convert to ISO string format
    return data


def send_data_to_kafka(api_url: str, topic: str, data: dict):
    # Convert datetime objects to string if present
    data = convert_datetime_fields(data)

    # use header to ensure correct data type is json
    headers = {"Content-Type": "application/json"}

    try:
        # Use json.dumps to convert dictionary to a properly formatted JSON string
        response = requests.post(
            f"{api_url}{topic}", data=json.dumps(data), headers=headers
        )
        response.raise_for_status()
        print(f"Successfully sent data to {topic}: {data}")
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
    except requests.exceptions.RequestException as err:
        print(f"Error sending data to {topic}: {err}")


if __name__ == "__main__":
    tables_to_topics = {
        "pinterest_data": "0affd83dcba5.pin",
        "geolocation_data": "0affd83dcba5.geo",
        "user_data": "0affd83dcba5.user",
    }
    creds_file = "db_creds.yaml"
    api_url = "https://l7v8sf1pt0.execute-api.us-east-1.amazonaws.com/test/topics/"
    run_infinite_post_data_loop(creds_file, api_url, tables_to_topics)
