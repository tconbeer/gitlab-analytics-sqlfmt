import logging
import os
import sys
import yaml
from time import time
from typing import Dict, List

import pandas as pd
from fire import Fire
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from common_utils import snowflake_engine_factory


def postgres_engine_factory(
    connection_dict: Dict[str, str], env: Dict[str, str]
) -> Engine:
    """
    Create a postgres engine to be used by pandas.
    """

    # Set the Vars
    user = env[connection_dict["user"]]
    password = env[connection_dict["pass"]]
    host = env[connection_dict["host"]]
    database = env[connection_dict["database"]]
    port = env[connection_dict["port"]]

    # Inject the values to create the engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    logging.info(engine)
    return engine


def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a manifest file into a dictionary and pass it back.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file)

    return manifest_dict


def query_results_generator(query: str, engine: Engine) -> pd.DataFrame:
    """
    Use pandas to run a sql query and load it into a dataframe.
    Yield it back in chunks for scalability.
    """

    query_df_iterator = pd.read_sql(sql=query, con=engine, chunksize=15000)
    return query_df_iterator


def main(
    *file_paths: List[str], incremental_only: bool = False, scd_only: bool = False
) -> None:
    """
    Read data from a postgres DB and write it to stdout following the
    Singer spec.
    """

    # Make sure that only one of the flags is set
    if incremental_only and scd_only:
        logging.info("Only one 'only' flag can be used at a time")
        sys.exit()

    # Set vars
    env = os.environ.copy()
    chunksize = 15000
    snowflake_schema = "tap_postgres"

    # Iterate through the manifests
    for file_path in file_paths:
        logging.info(f"Reading manifest at location: {file_path}")
        manifest_dict = manifest_reader(file_path)

        logging.info("Creating database engines...")
        connection_dict = manifest_dict["connection_info"]
        postgres_engine = postgres_engine_factory(connection_dict, env)
        snowflake_engine = snowflake_engine_factory(env, "LOADER")
        logging.info(snowflake_engine)

        for table in manifest_dict["tables"]:
            logging.info(f"Querying for table: {table}")
            table_dict = manifest_dict["tables"][table]
            table_name = "{import_db}_{export_table}".format(**table_dict).upper()

            # Format the query
            raw_query = table_dict["import_query"]

            # Check for incremental mode and skip queries that don't use execution date (non-incremental)
            if incremental_only and "{EXECUTION_DATE}" not in raw_query:
                results_chunker = []
            elif scd_only and "{EXECUTION_DATE}" in raw_query:
                results_chunker = []
            else:
                query = raw_query.format(**env)
                logging.info(query)
                results_chunker = query_results_generator(
                    query=query, engine=postgres_engine
                )

            chunk_counter = 0
            for chunk_df in results_chunker:
                row_count = chunk_df.shape[0]
                logging.info(f"Uploading {row_count} records to snowflake...")

                chunk_df["_uploaded_at"] = time()  # Add an uploaded_at column
                chunk_df = chunk_df.applymap(
                    lambda x: x if not isinstance(x, dict) else str(x)
                )  # convert dict to str to avoid snowflake errors
                chunk_df.to_sql(
                    name=table_name,
                    con=snowflake_engine,
                    schema=snowflake_schema,
                    if_exists="append",
                    index=False,
                    chunksize=chunksize,
                )

                chunk_counter += row_count
                logging.info(f"{chunk_counter} total records uploaded...")
            logging.info(f"Finished upload for table: {table}")
    return


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    Fire({"tap": main})
