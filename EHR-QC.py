import argparse
parser = argparse.ArgumentParser(prog='EHR-QC')
parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')
args = parser.parse_args()