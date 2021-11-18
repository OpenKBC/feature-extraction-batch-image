__author__ = "Junhee Yoon"
__version__ = "1.0.0"
__maintainer__ = "Junhee Yoon"
__email__ = "swiri021@gmail.com"

"""
Description: Mimic of notebook code for pipeline work, please see step1 in Jun notebook archive
"""

import os
import pandas as pd
import numpy as np
import argparse
from libraries.metaHandler import metaExt

from sys import argv
from libraries.statFunction import StatHandler

parser = argparse.ArgumentParser(prog='step4_actscoreDiff.py')
# Input data
parser.add_argument('-t','--type', dest='resultType', default='RR,CIS',\
     help='Result type, ex: long, healthy, "RR,CIS" ')
args = parser.parse_args()


# Copy of OpenKbcMSToolkit.py

if __name__ == "__main__":

    me = metaExt()

    SharedFilePath = os.environ['efspoint'] # Main data path here, goes to EFS volume
    metaName = os.environ['metafile'] # EPIC_HCvB_metadata_baseline_updated-share.csv
    msigFile = os.environ['msigDBPATH'] # msigdb.v7.4.entrez.gmt
    step1Input = os.environ['startFile'] # counts_vst_CD4.csv
    inputFile = SharedFilePath+os.path.basename(step1Input).replace('.csv', '.step3.csv') # replace to step3 input

    df = pd.read_csv(inputFile, engine='c', index_col=0).T.dropna() # Activation Score
    meta_data = pd.read_csv(SharedFilePath+metaName) # Meta data
    longDD_samples, shortDD_samples = me._LoadDiseaseDuration(df, meta_data, args.resultType)
    ranksumSig = StatHandler.calculate_ranksum(df, shortDD_samples, longDD_samples) # get ranksum result
    
    outputFile = SharedFilePath+os.path.basename(step1Input).replace('.csv', '.step4.csv') # replace to step4 output
    df.loc[ranksumSig["Names"].values.tolist()].to_csv(outputFile) # Writing
