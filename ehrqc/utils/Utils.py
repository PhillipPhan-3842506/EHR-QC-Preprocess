import psycopg2

from ehrqc import Config

import logging

log = logging.getLogger("EHR-QC")


def getConnection():

    # Connect to postgres with a copy of the MIMIC-III database
    con = psycopg2.connect(
        dbname=Config.db_details["sql_db_name"],
        user=Config.db_details["sql_user_name"],
        host=Config.db_details["sql_host_name"],
        port=Config.db_details["sql_port_number"],
        password=Config.db_details["sql_password"]
        )

    return con


def drawSummaryTable(df, tag, text, col):
    
    import numpy as np

    with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
        with tag('tr'):
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Statistics - ' + col)
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Minimum')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].min())))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('First Quartile')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].quantile(0.25), 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Mean')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].mean(), 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Median')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].median(), 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Mode')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].mode()[0], 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Third Quartile')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].quantile(0.75), 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Maximum')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].max())))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Standard Deviation')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].std(), 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Variance')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].var(), 2)))
        with tag('tr'):
            footnote = ''
            if np.absolute(df[col].skew()) > 2:
                footnote += '†'
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Skew')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].skew(), 2)) + footnote)
        with tag('tr'):
            footnote = ''
            if np.absolute(df[col].kurtosis()) > 10:
                footnote += '†'
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Kurtosis')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].kurtosis(), 2)) + footnote)
