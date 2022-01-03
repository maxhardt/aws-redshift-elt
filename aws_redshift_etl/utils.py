import configparser
import os

def load_config(config_filepath: str) -> configparser.ConfigParser():
    """Loads config from file using `configparser`.

    Args:
        config_filepath (str): Filepath to config file

    Returns:
        configparser.ConfigParser: ConfigParser object for reading configs.
    """
    config = configparser.ConfigParser()
    config.read(config_filepath)
    return config


def get_conn_string(config: configparser.ConfigParser):
    """Gets Redshift connection string from loaded config.

    Args:
        config (configparser.ConfigParser): Initialized config.

    Returns:
        str: Connection string for Redshift.
    """
    return "host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values())
