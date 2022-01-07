import argparse
import configparser
import logging

import psycopg2

from aws_redshift_etl.sql_queries import (copy_table_queries,
                                          insert_table_queries)
from aws_redshift_etl.utils import get_conn_string, load_config


def load_staging_tables(cur, conn, config):
    """Loads data from JSON files on S3 into staging tables.
    - Loads songs data into `staging.s_songs`
    - Loads events data into `staging.s_events`

    Args:
        cur (psycopg2.cursor): Cursor for executing queries.
        conn (psycopg2.connection): Database connection.
    """

    for query_func in copy_table_queries:
        query = query_func(config)
        logging.info(f"Attempting to execute query: {query}")
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(e)
        logging.info(f"Query finished successful")


def insert_tables(cur, conn):
    """Transforms data from staging tables into analytics tables.
    
    Transforms are `INSERT INTO` statements using staging tables as sources:
    - `d_users` from `s_events`
    - `d_time` from `s_events`
    - `d_artists` from `s_songs`
    - `d_songs` from `s_songs`
    - `f_songplays` from `s_events` and `s_songs`

    Args:
        cur (psycopg2.cursor): Cursor for executing queries.
        conn (psycopg2.connection): Database connection.
    """

    for query in insert_table_queries:
        logging.info(f"Attempting to execute query: {query}")
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(e)
        logging.info(f"Query finished successful")


def etl(config: configparser.ConfigParser, skip_staging: bool):

    # connect to redshift
    try:
        conn = psycopg2.connect(get_conn_string(config))
        cur = conn.cursor()
    except Exception as e:
        logging.error(f"Failed connecting to cluster: {e}")    
    logging.info("Successfully established connection to cluster.")

    # load staging tables
    if skip_staging:
        logging.info(f"Loading staging tables is skipped, existing data will be used.")
    else:
        logging.info(f"Loading staging tables, this might take around 2 hours...")
        load_staging_tables(cur, conn, config)

    logging.info(f"Transforming staging tables into analytics tables.")
    # transform staging into analytics tables
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--no-verbose', default=False, action='store_true',
        help="Enables verbose logging behaviour",
    )

    parser.add_argument(
        '--config', type=str, default="./dwh.cfg",
        help="Filepath to config file"
    )

    parser.add_argument(
        '--skip-staging', default=False, action='store_true',
        help="If given, do not load any staging data"
    )

    args = parser.parse_args()
    arguments = args.__dict__

    level = logging.INFO if not arguments["no_verbose"] else logging.WARN

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S',
        filename="./logs/etl.log"
    )

    config = load_config(arguments["config"])
    etl(config, arguments["skip_staging"])
