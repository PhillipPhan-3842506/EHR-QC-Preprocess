import os

import pandas as pd
import numpy as np

from pathlib import Path

from matplotlib import pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import seaborn as sns

from PIL import Image

from fpdf import FPDF


def plot(
    sourceFiles,
    colNamesDict,
    outputFile,
    labels
    ):

    log.info("Reading the source files")
    sourceDfs = []
    for sourceFile in sourceFiles:
        sourceFilePath = Path(sourceFile)
        if sourceFilePath.is_file():
            df = pd.read_csv(sourceFilePath)
            sourceDfs.append(df)

    pdf = FPDF()

    xSize = len(sourceDfs)
    for key in colNamesDict.keys():
        log.info("Plotting: " + str(key))
        plotDfs = []
        fig, ax = plt.subplots()
        for i in range(xSize):
            y = sourceDfs[i][colNamesDict[key][i]]
            df = pd.DataFrame({'x': labels[i], 'y': y})
            plotDfs.append(df)
        plotDf = pd.concat(plotDfs, ignore_index=True)
        sns.violinplot(
            data=plotDf
            , x='x'
            , y='y'
            , ax=ax
        )

        ax.set_title('Distribution Plot')
        ax.set_xlabel(str(key))
        ax.set_ylabel('Value')

        # Converting Figure to an image:
        canvas = FigureCanvas(fig)
        canvas.draw()
        img = Image.fromarray(np.asarray(canvas.buffer_rgba()))

        pdf.add_page(orientation='landscape')
        pdf.image(img, w=pdf.epw * 0.8)  # Make the image full width

    log.info("Saving the pdf")
    dirPath = Path(outputFile).parent
    if not os.path.exists(dirPath):
        os.makedirs(dirPath)
    pdf.output(outputFile)


if __name__ == '__main__':

    import json
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

    parser = argparse.ArgumentParser(description='Draw exploration graphs as pdf files')

    parser.add_argument('save_path', nargs=1, default='plot.html',
                        help='Path of the file to store the output')

    parser.add_argument('-c', '--columns', type=json.loads)

    parser.add_argument('-sf', '--source_file_list', nargs='*')

    parser.add_argument('-l', '--labels', nargs='*')

    args = parser.parse_args()

    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.columns: ' + str(args.columns))
    log.info('args.source_file_list: ' + str(args.source_file_list))
    log.info('args.labels: ' + str(args.labels))

    plot(sourceFiles=args.source_file_list, colNamesDict=args.columns, outputFile=args.save_path[0], labels=args.labels)

    log.info('Done!!')
