import logging

log = logging.getLogger("EHR-QC")

import sys

import re

import numpy as np
import pandas as pd
import os
from pathlib import Path
from sklearn.metrics import r2_score

import impyute as impy
import miceforest as mf
from sklearn.impute import SimpleImputer
from sklearn.impute import KNNImputer
from sklearn.impute import SimpleImputer
from ehrqc.utils.MissForest import MissForest

import warnings

warnings.filterwarnings("ignore")


def compare(fullDf, p=None):

    if not p:
        if fullDf.isna().sum().mean() > 0:
            p = fullDf.isna().sum().mean() / fullDf.shape[0]
    else:
        p = float(p[0])

    log.info("Creating missingness with proportion p = (" + str(p) + ")")

    log.info("Data size before dropping nulls: " + str(fullDf.shape))
    fullDf.dropna(inplace=True)
    log.info("Data size after dropping nulls: " + str(fullDf.shape))

    mask = np.random.choice(a=[True, False], size=fullDf.shape, p=[p, 1 - p])
    missingDf = fullDf.mask(mask)
    missingDf = missingDf.rename(columns=lambda x: re.sub("[^A-Za-z0-9_]+", "", x))

    try:
        log.info("Running mean imputation")
        meanImputer = SimpleImputer(strategy="mean")
        meanImputedData = meanImputer.fit_transform(missingDf)
        meanImputedDf = pd.DataFrame(
            meanImputedData,
            columns=[(col + "_mean") for col in missingDf.columns],
            index=missingDf.index,
        )
        meanR2 = r2_score(fullDf, meanImputedDf)
        log.info("Mean imputation R2 score: " + str(meanR2))
    except:
        log.exception("Mean imputation failed!!")
        meanR2 = 0

    try:
        log.info("Running median imputation")
        medianImputer = SimpleImputer(strategy="median")
        medianImputedData = medianImputer.fit_transform(missingDf)
        medianImputedDf = pd.DataFrame(
            medianImputedData,
            columns=[(col + "_median") for col in missingDf.columns],
            index=missingDf.index,
        )
        medianR2 = r2_score(fullDf, medianImputedDf)
        log.info("Median imputation R2 score: " + str(medianR2))
    except:
        log.exception("Median imputation failed!!")
        medianR2 = 0

    try:
        log.info("Running KNN imputation")
        knnImputer = KNNImputer()
        knnImputedData = knnImputer.fit_transform(missingDf)
        knnImputedDf = pd.DataFrame(
            knnImputedData,
            columns=[(col + "_knn") for col in missingDf.columns],
            index=missingDf.index,
        )
        knnR2 = r2_score(fullDf, knnImputedDf)
        log.info("KNN imputation R2 score: " + str(knnR2))
    except:
        log.exception("KNN imputation failed!!")
        knnR2 = 0

    try:
        log.info("Running MissForest imputation")
        mfImputer = MissForest()
        mfImputedData = mfImputer.fit(missingDf).transform(missingDf)
        mfImputedDf = pd.DataFrame(
            mfImputedData,
            columns=[(col + "_mf") for col in missingDf.columns],
            index=missingDf.index,
        )
        mfR2 = r2_score(fullDf, mfImputedDf)
        log.info("MissForest imputation R2 score: " + str(mfR2))
    except:
        log.exception("MissForest imputation failed!!")
        mfR2 = 0

    try:
        log.info("Running EM imputation")
        emImputedData = impy.em(missingDf.to_numpy())
        emImputedDataDf = pd.DataFrame(
            emImputedData,
            columns=[(col + "_em") for col in missingDf.columns],
            index=missingDf.index,
        )
        emR2 = r2_score(fullDf, emImputedDataDf)
        log.info("EM imputation R2 score: " + str(emR2))
    except:
        log.exception("EM imputation failed!!")
        emR2 = 0

    try:
        log.info("Running MI imputation")
        miKernel = mf.ImputationKernel(
            missingDf, datasets=4, save_all_iterations=True, random_state=1
        )
        miKernel.mice(2)
        miImputedDataDf = miKernel.complete_data(dataset=0, inplace=False)
        miImputedDf = pd.DataFrame(
            miImputedDataDf.to_numpy(),
            columns=[(col + "_mi") for col in missingDf.columns],
            index=missingDf.index,
        )
        miR2 = r2_score(fullDf, miImputedDf)
        log.info("MI imputation R2 score: " + str(miR2))
    except:
        log.exception("MI imputation failed!!")
        miR2 = 0

    return meanR2, medianR2, knnR2, mfR2, emR2, miR2


def impute(dataDf, algorithm):

    imputedDf = None
    if algorithm == "mean":
        meanImputer = SimpleImputer(strategy="mean")
        meanImputedData = meanImputer.fit_transform(dataDf)
        imputedDf = pd.DataFrame(
            meanImputedData, columns=dataDf.columns, index=dataDf.index
        )
    if algorithm == "median":
        medianImputer = SimpleImputer(strategy="median")
        medianImputedData = medianImputer.fit_transform(dataDf)
        imputedDf = pd.DataFrame(
            medianImputedData, columns=dataDf.columns, index=dataDf.index
        )
    if algorithm == "knn":
        knnImputer = KNNImputer()
        knnImputedData = knnImputer.fit_transform(dataDf)
        imputedDf = pd.DataFrame(
            knnImputedData, columns=dataDf.columns, index=dataDf.index
        )
    if algorithm == "miss_forest":
        mfImputer = MissForest()
        mfImputedData = mfImputer.fit(dataDf).transform(dataDf)
        imputedDf = pd.DataFrame(
            mfImputedData, columns=dataDf.columns, index=dataDf.index
        )
    if algorithm == "expectation_maximization":
        emImputedData = impy.em(dataDf.to_numpy())
        imputedDf = pd.DataFrame(
            emImputedData, columns=dataDf.columns, index=dataDf.index
        )
    if algorithm == "multiple_imputation":
        miKernel = mf.ImputationKernel(
            dataDf, datasets=4, save_all_iterations=True, random_state=1
        )
        miKernel.mice(2)
        miImputedDataDf = miKernel.complete_data(dataset=0, inplace=False)
        imputedDf = pd.DataFrame(
            miImputedDataDf.to_numpy(), columns=dataDf.columns, index=dataDf.index
        )

    return imputedDf


def run(
    action="compare",
    source_path="data.csv",
    p=None,
    save_path="imputed.csv",
    algorithm="mean",
    columns=[],
):
    dataDf = pd.read_csv(source_path)
    print(dataDf.columns)

    if columns and len(columns) > 0:
        if action == "compare":
            meanR2, medianR2, knnR2, mfR2, emR2, miR2 = compare(
                fullDf=dataDf[columns], p=p
            )
            log.info(
                "mean R2: "
                + str(meanR2)
                + ", median R2: "
                + str(medianR2)
                + ", knn R2: "
                + str(knnR2)
                + ", mf R2: "
                + str(mfR2)
                + ", em R2: "
                + str(emR2)
                + ", mi R2: "
                + str(miR2)
            )
        elif action == "impute":
            imputedDf = impute(dataDf=dataDf[columns], algorithm=algorithm)
            otherDf = dataDf[dataDf.columns[~dataDf.columns.isin(columns)]]
            finalDf = pd.concat([otherDf, imputedDf], axis=1)

            dirPath = Path(save_path).parent
            if not os.path.exists(dirPath):
                os.makedirs(dirPath)

            finalDf.to_csv(Path(save_path), index=None)
            # finalDf.to_csv(save_path, index=None)


if __name__ == "__main__":

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

    parser = argparse.ArgumentParser(description="EHR-QC")

    parser.add_argument(
        "source_path", nargs=1, default="data.csv", help="Source data path"
    )

    parser.add_argument(
        "-ac",
        "--action",
        nargs=1,
        default="compare",
        help="Action to perform [compare, impute]",
    )

    parser.add_argument(
        "-p",
        "--percentage",
        nargs=1,
        help="Missing value proportion for comparison (required only for action=compare)",
    )

    parser.add_argument(
        "-sp",
        "--save_path",
        nargs=1,
        default="imputed.csv",
        help="Path of the file to store the outputs (required only for action=impute)",
    )

    parser.add_argument(
        "-al",
        "--algorithm",
        nargs=1,
        default="mean",
        help="Missing data imputation algorithm [mean, median, knn, miss_forest, expectation_maximization, multiple_imputation]",
    )

    parser.add_argument("-c", "--columns", nargs="*", help="Columns to impute")

    args = parser.parse_args()

    log.info("args.action: " + str(args.action[0]))
    log.info("args.source_path: " + str(args.source_path[0]))
    log.info("args.percentage: " + str(args.percentage))
    log.info("args.save_path: " + str(args.save_path[0]))
    log.info("args.algorithm: " + str(args.algorithm[0]))
    log.info("args.columns: " + str(args.columns))

    run(
        action=args.action[0],
        source_path=args.source_path[0],
        p=args.percentage,
        save_path=args.save_path[0],
        algorithm=args.algorithm[0],
        columns=args.columns,
    )

    log.info("Done!!")
