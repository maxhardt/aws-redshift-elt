import argparse
import configparser
import logging

import psycopg2

from aws_redshift_etl.queries import (create_table_queries, drop_table_queries,
                                      drop_table_queries_no_staging)
from aws_redshift_etl.utils import get_conn_string, load_config


def drop_tables(cur, conn, skip_staging: bool):
    """Drops all tables from staging and analytics schemas.

    Args:
        cur (psycopg2.cursor): Cursor for executing queries.
        conn (psycopg2.connection): Database connection.
        drop_staging (bool, optional): If True, also drops staging tables.
            Warning: These may take a lot of time to reload. Defaults to True.
    """

    if skip_staging:
        drop_queries = drop_table_queries_no_staging
    else: 
        drop_queries = drop_table_queries

    for query in drop_queries:
        logging.info(f"Attempting to execute query: {query}")
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(e)
        logging.info(f"Query finished successful")


def create_tables(cur, conn):
    """Creates all tables schemas in staging and analytics schemas.

    Uses `IF EXIST` clause in all queries.

    Args:
        cur (psycopg2.cursor): Cursor for executing queries.
        conn (psycopg2.connection): Database connection.
    """

    for query in create_table_queries:
        logging.info(f"Attempting to execute query: {query}")
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            logging.error(e)
        logging.info(f"Query finished successful")


def drop_and_create(config: configparser.ConfigParser, skip_staging: bool):

    # connect to redshift
    try:
        conn = psycopg2.connect(get_conn_string(config))
        cur = conn.cursor()
    except Exception as e:
        logging.error(f"Failed connecting to cluster: {e}")    
    logging.info("Successfully established connection to cluster.")

    # drop_staging -> `False` to prevent reloading of raw data
    logging.info(f"Dropping tables with `skip_staging` set to: {skip_staging}")
    drop_tables(cur, conn, skip_staging)

    # create all table schemas
    create_tables(cur, conn)
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
        help="If given, do not drop staging tables"
    )

    args = parser.parse_args()
    arguments = args.__dict__

    level = logging.INFO if not arguments["no_verbose"] else logging.WARN

    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S',
        filename="./logs/create_tables.log"
    )

    config = load_config(arguments["config"])
    drop_and_create(config, arguments["skip_staging"])
