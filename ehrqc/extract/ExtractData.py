import os
import pandas as pd


def extractIcdCohort(con, schemaName, sqlFilePath):
    curDir = os.path.dirname(__file__)
    mimicOmopSepsisIcdPath = os.path.join(curDir, sqlFilePath)
    mimicOmopSepsisIcdFile = open(mimicOmopSepsisIcdPath)
    mimicOmopSepsisIcdQuery = mimicOmopSepsisIcdFile.read()
    mimicOmopSepsisIcdQuery = mimicOmopSepsisIcdQuery.replace('__schema_name__', schemaName)
    mimicOmopSepsisIcdDf = pd.read_sql_query(mimicOmopSepsisIcdQuery, con)
    return mimicOmopSepsisIcdDf


def extract(con, sqlFilePath, savePath='data/cohort.csv', schemaName = 'mimiciv'):
    log.info('extracting data')
    data = extractIcdCohort(con, schemaName = schemaName, sqlFilePath = sqlFilePath)
    if data is not None:
        log.info('Saving raw data to file')
        data.to_csv(savePath, index=False)
    else:
        log.error('Unable to extract data, please check the parametrs and try again!')


if __name__ == '__main__':

    import logging
    import sys
    import argparse

    log = logging.getLogger("EHR-QC")
    log.setLevel(logging.INFO)
    format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(format)
    log.addHandler(ch)

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='EHR-QC preprocessing utility')

    parser.add_argument('save_path', nargs=1, default='data/cohort.csv',
                        help='Path of the file to store the outputs')

    parser.add_argument('schema_name', nargs=1, default='mimiciv',
                        help='Source schema name')

    parser.add_argument('sql_file_path', nargs=1,
                        help='Path of the file containing SQL query')

    args = parser.parse_args()

    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.schema_name: ' + str(args.schema_name[0]))
    log.info('args.sql_file_path: ' + str(args.sql_file_path[0]))

    from ehrqc.utils.Utils import getConnection

    con = getConnection()

    extract(con=con, sqlFilePath = args.sql_file_path[0], savePath=args.save_path[0], schemaName = args.schema_name[0])
