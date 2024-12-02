import logging
import yaml
from user_posting_emulation import AWSDBConnector, run_infinite_post_data_loop


def connection(file_path):
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            creds = yaml.safe_load(file)
            if not isinstance(creds, dict):
                raise ValueError("Invalid YAML format.")

            required_keys = {
                "RDS_USER",
                "RDS_PASSWORD",
                "RDS_HOST",
                "RDS_PORT",
                "RDS_DATABASE",
            }
            if not required_keys.issubset(creds.keys()):
                raise ValueError("Missing required database credentials.")
            return creds
    except FileNotFoundError:
        logger.error("Credentials file not found.")
        raise
    except yaml.YAMLError:
        logger.error("Error parsing the YAML file.")
        raise
    except ValueError as e:
        logger.error(str(e))
        raise


new_connector = AWSDBConnector()

run_infinite_post_data_loop()
