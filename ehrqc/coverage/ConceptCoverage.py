import os
import psycopg2

import pandas as pd

from pathlib import Path
import logging
import sys
import argparse

log = logging.getLogger("EHR-QC")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)


def getConnection():

    # con = psycopg2.connect(
    #     dbname=os.environ['POSTGRES_DB_NAME'],
    #     user=os.environ['POSTGRES_USER_NAME'],
    #     host=os.environ['POSTGRES_HOSTNAME'],
    #     port=os.environ['POSTGRES_PORT_NUMBER'],
    #     password=os.environ['POSTGRES_PASSWORD']
    #     )

    con = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        # host="localhost",
        host="db",
        port="5432",
        password="mypassword",
    )

    return con


def run(schema_name, save_file):

    log.info("Obtaiting Coverage from DB")

    con = getConnection()

    coverageQuery = (
        """
        select
        con.concept_name,
        con.concept_code,
        (count(distinct mmt.person_id)::float * 100)/(select count(distinct person_id) from """
        + schema_name
        + """.cdm_person) as person_level_coverage,
        (count(distinct mmt.visit_occurrence_id)::float * 100)/(select count(distinct visit_occurrence_id) from """
        + schema_name
        + """.cdm_visit_occurrence) as episode_level_coverage
        from
        """
        + schema_name
        + """.cdm_measurement mmt
        inner join """
        + schema_name
        + """.voc_concept con
        on con.concept_code = mmt.measurement_concept_id::text
        where mmt.unit_id like '%labevents%'
        group by con.concept_name, con.concept_code order by person_level_coverage desc;
    """
    )
    coverageDf = pd.read_sql_query(coverageQuery, con)

    dirPath = Path(save_file).parent
    if not os.path.exists(dirPath):
        os.makedirs(dirPath)

    coverageDf.to_csv(save_file, index=False)


if __name__ == "__main__":

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description="Perform Coverage Analysis")
    parser.add_argument("schema_name", nargs=1, help="Name of the OMOP DB schema")
    parser.add_argument(
        "-sf",
        "--save_file",
        nargs=1,
        default="./concept_coverage.csv",
        help="Path of the file to store the output",
    )

    args = parser.parse_args()

    log.info("Start!!")
    log.info("args.schema_name: " + str(args.schema_name[0]))
    log.info("args.save_file: " + str(args.save_file[0]))

    run(
        schema_name=args.schema_name[0],
        save_file=args.save_file[0],
    )

    log.info("Done!!")
