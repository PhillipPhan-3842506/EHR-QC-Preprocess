import os, tempfile, subprocess
import time
import base64

import pandas as pd
import numpy as np
from scipy import stats

from pathlib import Path

from sklearn.decomposition import PCA

from yattag import Doc

doc, tag, text = Doc().tagtext()

from ehrqc import Settings


def clean(source_file, save_file, columns):

    outputFile = Path(save_file)
    dataDf = pd.read_csv(source_file)
    log.info('Validating the input arguments.')

    if(dataDf[columns].shape[1] > Settings.col_limit_for_outlier_plot):
        log.info('Too many variables to plot!! Please sleect the vaiables to plot using combinations argument.')
        return
    elif (dataDf.shape[0] * dataDf.shape[1]) > int(Settings.cell_limit):
        log.info('This file has ' + str(dataDf.shape[0] * dataDf.shape[1]) + ' cells.')
        log.info('The maximum number of cell that can be passed to this pipeline is ' + str(Settings.cell_limit))
        log.info('File too big to handle!! Please remove the columns with low coverage and try again.')
        log.info('Refer to this link: https://ehr-qc-tutorials.readthedocs.io/en/latest/process.html#large-file-handling')
        return
    log.info('Validating complete!!')

    log.info('Removing outliers')
    dataDf = dataDf[columns]
    if dataDf is not None:
        if not dataDf.empty:
            outliersDf = irt_ensemble(dataDf)
            outliersDf = outliersDf[(np.abs(stats.zscore(outliersDf.ensemble_scores)) < Settings.ensemble_score_threshold)]
            outliersDf.drop('y_knn_agg', axis=1, inplace=True)
            outliersDf.drop('y_lof', axis=1, inplace=True)
            outliersDf.drop('y_inflo', axis=1, inplace=True)
            outliersDf.drop('y_kdeos', axis=1, inplace=True)
            outliersDf.drop('y_ldf', axis=1, inplace=True)
            outliersDf.drop('ensemble_scores', axis=1, inplace=True)

    outliersDf.to_csv(outputFile, index=False)


def visualise(source_file, save_file, combinations, columns):

    outputFile = Path(save_file)
    dataDf = pd.read_csv(source_file)
    log.info('Validating the input arguments.')

    if(columns and (dataDf[columns].shape[1] > Settings.col_limit_for_outlier_plot)):
        log.info('Too many variables to plot!! Please sleect the vaiables to plot using combinations argument.')
        return
    elif(combinations and (len(combinations) > Settings.combinations_limit_for_outlier_plot)):
        log.info('Too many variables to plot!! The number of combination should not be more than 6.')
        return
    elif (dataDf.shape[0] * dataDf.shape[1]) > int(Settings.cell_limit):
        log.info('This file has ' + str(dataDf.shape[0] * dataDf.shape[1]) + ' cells.')
        log.info('The maximum number of cell that can be passed to this pipeline is ' + str(Settings.cell_limit))
        log.info('File too big to handle!! Please remove the columns with low coverage and try again.')
        log.info('Refer to this link: https://ehr-qc-tutorials.readthedocs.io/en/latest/process.html#large-file-handling')
        return
    log.info('Validating complete!!')

    start = time.time()

    log.info('Generating outlier report')
    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        doc.asis('<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-F3w7mX95PdgyTmZZMECAngseQB83DfGTowi0iMjiWaeVhAn4FJkqJByhZMI3AhiU" crossorigin="anonymous">')
        with tag('body'):
            if combinations:
                for (x, y) in combinations:
                    if x in dataDf.columns and y in dataDf.columns:
                        with tag('div'):
                            with tag('h1'):
                                doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                                with tag('span', klass='fs-4', style="margin: 10px;"):
                                    text('Outliers plot - ' + x + ' - ' + y)
                                    log.info('Generating outlier plot for columns: ' + x + ' - ' + y)
                            fig = __drawOutliers(dataDf, [x, y], outputFile)
                            if fig:
                                with tag('div', style="float: left;"):
                                    doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(fig))
                    doc.asis('<div style="clear:both;"></div>')
            elif columns:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Outliers plot')
                            log.info('Generating outlier plot for columns: ' + str(columns))
                    fig = __drawOutliers(dataDf, columns, outputFile)
                    if fig:
                        with tag('div', style="float: left;"):
                            doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(fig))
                doc.asis('<div style="clear:both;"></div>')
            doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('Time taken to generate this report: ' + str(round(time.time() - start, 2)) + ' Sec')
            doc.asis('<div style="clear:both;"></div>')
    log.info('Report generated!!')

    log.info('Writing output file')
    with open(outputFile, 'w') as output:
        output.write(doc.getvalue())
    log.info('Completed writing output file!!')


def __drawOutliers(df, columns, outputFile):

    from matplotlib import pyplot as plt
    import seaborn as sns

    if len(columns) < 2:
        log.info('Can not generate outlier plot due to insufficient number of attributes (<2)')
    elif len(columns) == 2:
        plotdf = df[columns]
    else:
        plotdf = pd.DataFrame(PCA(2).fit(df).transform(df), columns=['component_1', 'component_2'])
    outliersDf = irt_ensemble(plotdf)

    if outliersDf is not None and len(outliersDf) > 1:
        fig, ax = plt.subplots()
        x_o = outliersDf.columns[0].replace(' ', '.')
        y_o = outliersDf.columns[1].replace(' ', '.')
        sns.scatterplot(data=outliersDf, x=x_o, y=y_o, hue='ensemble_scores')

        ax.set_title('Scatter plot')
        ax.set_xlabel(outliersDf.columns[0])
        ax.set_ylabel(outliersDf.columns[1])

        encoded = None
        outPath = Path(Path(outputFile).parent, 'outlier_plot_' + outliersDf.columns[0] + '_' + outliersDf.columns[1] + '.png')
        fig.savefig(outPath, format='png', bbox_inches='tight')
        with open(outPath, "rb") as outFile:
            encoded = base64.b64encode(outFile.read()).decode('utf-8')
    else:
        encoded = None

    return encoded


def irt_ensemble(data):

    inFile = tempfile.NamedTemporaryFile(delete=False)
    outFile = tempfile.NamedTemporaryFile(delete=False)
    try:
        data.to_csv(inFile, index=False)
        subprocess.call (["/usr/bin/Rscript", "--vanilla", "ehrqc/utils/script.r", inFile.name, outFile.name])

        if outFile:
            try:
                out = pd.read_csv(outFile)
            except:
                out = None
        else:
            out = None

    finally:
        inFile.close()
        outFile.close()
        os.unlink(inFile.name)
        os.unlink(outFile.name)

    return out


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

    parser = argparse.ArgumentParser(description='Outlier graphs using IRT ensemble technique')

    parser.add_argument('source_file', nargs=1, default='./data.csv',
                        help='Source data file path')

    parser.add_argument('save_file', nargs=1, default='./',
                        help='Path of the file to store the output')

    parser.add_argument('action', nargs=1, default='visualise',
                        help='Action to perform [visualise, clean]')

    parser.add_argument('-com', '--combinations', nargs='*', action='append',
                        help='Column combinations to plot - can have multiple column pairs (required only for action=visualise).')

    parser.add_argument('-col', '--columns', nargs='*',
                        help='Column names to be used for outlier detection - must have two or more column names (required for both actions i.e. visualise and clean).')

    args = parser.parse_args()

    log.info('args.source_file: ' + str(args.source_file[0]))
    log.info('args.save_file: ' + str(args.save_file[0]))
    log.info('args.action: ' + str(args.action[0]))
    log.info('args.combinations: ' + str(args.combinations))
    log.info('args.columns: ' + str(args.columns))

    if args.action[0] == 'clean':
        clean(source_file=args.source_file[0], save_file=args.save_file[0], columns=args.columns)
    else:
        visualise(source_file=args.source_file[0], save_file=args.save_file[0], combinations=args.combinations, columns=args.columns)

    log.info('Done!!')
