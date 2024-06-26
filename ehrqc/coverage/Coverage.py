import os
import logging
import sys

import argparse

log = logging.getLogger("EHR-QC")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)


import pandas as pd
import numpy as np

from pathlib import Path


def calculateMissingness(source_file, chunksize, id_columns):
    chunksize = chunksize
    source_df_list = []
    missing_df_list = []
    for df in pd.read_csv(source_file, chunksize=chunksize):

        source_df_list.append(df)
        df.replace("", np.nan, inplace=True)
        df = df.dropna(subset=df.columns[df.columns.isin(id_columns)], how="any")
        missing_counts = (df.groupby(id_columns).agg("count") == 0).sum()
        missing_df = pd.DataFrame(
            {
                "column_name": missing_counts.index,
                "missing_count": missing_counts,
                "total_count": len(missing_counts.index)
                * [df[id_columns].drop_duplicates().shape[0]],
            }
        )
        missing_df_list.append(missing_df)
    final_df = pd.concat(missing_df_list, ignore_index=True)
    final_agg_df = final_df.groupby(["column_name"]).sum().reset_index()
    final_agg_df["percentage_missing"] = round(
        final_agg_df["missing_count"] / final_agg_df["total_count"] * 100, 2
    )
    return final_agg_df, source_df_list


def run(source_file, chunksize, id_columns, drop, percentage, save_path):

    log.info("Calculating Missingnes")
    missing_df, source_df_list = calculateMissingness(
        source_file=source_file, chunksize=chunksize, id_columns=id_columns
    )

    log.info("Missingness Report")
    pd.set_option("display.max_columns", None)
    log.info("\n\n" + str(missing_df) + "\n")

    if drop:
        log.info(
            """Dropping columns with above """ + str(percentage) + """ % missingness"""
        )
        source_dropped_df_list = []
        i = 0
        for source_df in source_df_list:
            i = i + 1
            source_df.drop(
                missing_df[missing_df.percentage_missing > percentage].column_name,
                axis=1,
                inplace=True,
            )
            tresh = int(source_df.shape[1] / 2)
            source_df = source_df.dropna(thresh=tresh)
            source_dropped_df_list.append(source_df)
        source_dropped_df = pd.concat(source_dropped_df_list, ignore_index=True)
        log.info("""Saving data to """ + save_path)

    dirPath = Path(save_path).parent
    if not os.path.exists(dirPath):
        os.makedirs(dirPath)

    source_dropped_df.to_csv(Path(save_path), index=None)


if __name__ == "__main__":

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description="Perform Coverage Analysis")
    parser.add_argument(
        "source_file", nargs=1, default="data.csv", help="Source data file path"
    )
    parser.add_argument(
        "chunksize",
        nargs=1,
        type=int,
        default=100000,
        help="Number of chunks the input file should be fragmented into. By default: [chunksize=100000]",
    )
    parser.add_argument(
        "-i",
        "--id_columns",
        nargs="+",
        help="List of ID columns. They are used to group the other columns to calculate missing percentage.",
    )
    parser.add_argument("-d", "--drop", action="store_true", help="Drop the columns")
    parser.add_argument(
        "-p",
        "--percentage",
        nargs=1,
        type=float,
        default=[100],
        help="Specify the cutoff percentage to drop the columns (required only for drop=True). By default: [-p=100]",
    )
    parser.add_argument(
        "-sp",
        "--save_path",
        nargs=1,
        default="./",
        help="Path of the file to store the outputs (required only for drop=True)",
    )

    args = parser.parse_args()

    log.info("Start!!")
    log.info("args.source_file: " + str(args.source_file[0]))
    log.info("args.chunksize: " + str(args.chunksize[0]))
    log.info("args.id_columns: " + str(args.id_columns))
    log.info("args.drop: " + str(args.drop))
    log.info("args.percentage: " + str(args.percentage[0]))
    log.info("args.save_path: " + str(args.save_path[0]))

    run(
        source_file=args.source_file[0],
        chunksize=args.chunksize[0],
        id_columns=args.id_columns,
        drop=args.drop,
        percentage=args.percentage[0],
        save_path=args.save_path[0],
    )

    log.info("Done!!")
