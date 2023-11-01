import time
import base64
from matplotlib import pyplot as plt
import seaborn as sns
from yattag import Doc
from pathlib import Path

from ehrqc.utils.Utils import drawSummaryTable


doc, tag, text = Doc().tagtext()


def plot(
    df,
    colNames,
    outputFile = 'plots.html',
    ):

    if not colNames:
        colNames = df.columns

    start = time.time()

    log.info('Generating the exploration report')
    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        doc.asis('<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-F3w7mX95PdgyTmZZMECAngseQB83DfGTowi0iMjiWaeVhAn4FJkqJByhZMI3AhiU" crossorigin="anonymous">')
        with tag('body'):
            doc.asis('<div style="clear:both;"></div>')
            for col in colNames:
                if col in df.columns:
                    log.info('Columns: ' + col)
                    with tag('div'):
                        with tag('h1'):
                            doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                            with tag('span', klass='fs-4', style="margin: 10px;"):
                                text('Distribution - ' + col)
                        with tag('div', klass='col-5', style="float: left;"):
                            doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawViolinPlot(df, col, outputFile)))
                        with tag('div', klass='col-2', style="float: left;"):
                            drawSummaryTable(df, tag, text, col)
                doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('† - The data appears to be skewed, it is recommended to trim the extended tail before proceeding')
            with tag('div'):
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('Time taken to generate this report: ' + str(round(time.time() - start, 2)) + ' Sec')
            doc.asis('<div style="clear:both;"></div>')
    log.info('Saving the exploration report!!')
    with open(outputFile, 'w') as output:
        output.write(doc.getvalue())


def __drawViolinPlot(df, col, outputFile):

    fig, ax = plt.subplots()
    sns.violinplot(
        y = df[col]
        , ax=ax
    )

    ax.set_title('Violin Plot - ' + col)
    ax.set_xlabel(col)
    ax.set_ylabel('Value')

    encoded = None
    outPath = Path(Path(outputFile).parent, 'plot_' + col + '.png')
    fig.savefig(outPath, format='png', bbox_inches='tight')
    with open(outPath, "rb") as outFile:
        encoded = base64.b64encode(outFile.read()).decode('utf-8')

    return encoded


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

    parser = argparse.ArgumentParser(description='Draw exploration graphs')

    parser.add_argument('source_path', nargs=1, default='data.csv',
                        help='Source data path')

    parser.add_argument('save_path', nargs=1, default='plot.html',
                        help='Path of the file to store the output')

    parser.add_argument('-c', '--columns_list', nargs='*')

    args = parser.parse_args()

    log.info('args.source_path: ' + str(args.source_path[0]))
    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.columns_list: ' + str(args.columns_list))

    import pandas as pd

    df = pd.read_csv(args.source_path[0])
    plot(df=df, colNames=args.columns_list, outputFile=args.save_path[0])

    log.info('Done!!')
