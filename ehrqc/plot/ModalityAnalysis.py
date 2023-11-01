from unidip import UniDip

import time

import pandas as pd
import numpy as np

from yattag import Doc

import logging

log = logging.getLogger("EHR-QC")


doc, tag, text = Doc().tagtext()


def plot(
    df,
    colNames,
    outputFile = 'plots.html',
    ):

    if not colNames:
        colNames = df.columns

    start = time.time()

    log.info('Generating the modality analysis report')
    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        doc.asis('<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-F3w7mX95PdgyTmZZMECAngseQB83DfGTowi0iMjiWaeVhAn4FJkqJByhZMI3AhiU" crossorigin="anonymous">')
        with tag('body'):
            doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('h1'):
                    doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                    with tag('span', klass='fs-4', style="margin: 10px;"):
                        text('Modality Analysis')
                with tag('div', klass='col-2', style="float: left;"):
                    drawErrorAnalysis(df, df.columns, tag, text)
            doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('Time taken to generate this report: ' + str(round(time.time() - start, 2)) + ' Sec')
            doc.asis('<div style="clear:both;"></div>')
    log.info('Saving the modality analysis report!!')
    with open(outputFile, 'w') as output:
        output.write(doc.getvalue())


def drawErrorAnalysis(df, colNames, tag, text):

    with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
        with tag('tr'):
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Column')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Modality')
        isMultimodal = False
        for col in colNames:
            if col in df.columns:
                with tag('tr'):
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(col)
                    try:
                        dat = np.sort(df[col], axis=0)
                        intervals = UniDip(dat).run()
                    except:
                        intervals = []
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        if len(intervals) > 1:
                            isMultimodal = True
                            text('Multimodal')
                        else:
                            text('Unimodal')

    if isMultimodal:
        with tag('div'):
            with tag('span', klass='description', style="margin: 10px; color:red"):
                text('The data has columns with multimodal distributions, it might be due to mixing data from multiple sources or measurement units')


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

    parser = argparse.ArgumentParser(description='Perform modality analysis')

    parser.add_argument('source_path', nargs=1, default='data.csv',
                        help='Source data path')

    parser.add_argument('save_path', nargs=1, default='plot.html',
                        help='Path of the file to store the output')

    parser.add_argument('-c', '--columns_list', nargs='*')

    args = parser.parse_args()

    log.info('args.source_path: ' + str(args.source_path[0]))
    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.columns_list: ' + str(args.columns_list))

    df = pd.read_csv(args.source_path[0])
    plot(df=df, colNames=args.columns_list, outputFile=args.save_path[0])

    log.info('Done!!')
