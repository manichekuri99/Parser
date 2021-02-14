import sys
import re
import os
import numpy as np
import pandas as pd
import hashlib
from datetime import datetime
import string

from Spell.spell_parser import *
from Spell.post_process import *


input_dir  = '../22-may/'  # The input directory of log file
log_file   = 'bootstrap-armada.log'  # The input log file name
output_dir = 'Spell_result/'  # The output directory of parsing results

log_format = '<Date> <Timestamp> <EventCode> <MessageType> <Action> <Delim> <Content>'  # Number of Columns you want it parsed to <Content> is fixed
#log_format = '<Date> <Content>'  # Number of Columns you want it parsed to <Content> is fixed
tau = 0.58  # Message type threshold (default: 0.5)
regex = [
              r'blk_(|-)[0-9]+' , # block id
              r'[0-9]+(?:\.[0-9]+){3}(:[0-9]+)?',# IP
              r'(\d+\.){3}\d+(:\d+)?',
              r'\[.*?\]', #things in square bracket
              r'\((.*?)\)' 
] # Regular expression list for 'optional' preprocessing 

try:
    parser = LogParser(indir=input_dir, outdir=output_dir, log_format=log_format, tau=tau, rex=regex)
    parser.parse(log_file)
except:
    print("Parsing Failed, Please check Parsing Function in the module")
#POST PROCESSING

input_file_path = output_dir+log_file +'_structured.csv'

output_path = output_dir+'clean_formated_'+ log_file +'_structured.csv'

no_of_splits = 4
try: 
    post_process(input_file_path,no_of_splits,output_path)
    print("Post Processing Completed !")
except:
    print("Post Processing Failed, Please check Post process Function in the module")
