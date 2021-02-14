import sys
import re
import os
import numpy as np
import pandas as pd
import hashlib
from datetime import datetime
import string


def _clean_key_column(val):
    if val == '' or val == ' ' or val == '  ' :
        return 'Not Present'
    else:
        return val


def _clean_val_column(val):
    if val == '' or val == ' ' or val == '  ' :
        return 'None'
    else:
        return val



def post_process(file_path,no_of_splits,output_path):
    df = pd.read_csv(file_path)
    df['Params'] = df['ParameterList'].apply(lambda x: eval(x))
    df['CleanEventTemplate'] = df['EventTemplate'].apply(lambda x: x.replace("\x1b[00m",""))
    df['CleanedParamList'] = df['Params'].apply(lambda x: [i.replace("\x1b[00m","") for i in x])
    key_splits = [x.split('<*>',no_of_splits) for x in df['CleanEventTemplate'].tolist()]
    for key in key_splits:
        if len(key)<=no_of_splits:
            nw = ['NotPresent']*(no_of_splits-len(key))
            key.extend(nw)
        else:
            key = key[:no_of_splits]

    val_splits = []
    for x in df['CleanedParamList'].tolist():
        if len(x)<=no_of_splits:
            nw = ['None']*(no_of_splits-len(x))
            x.extend(nw)
            val_splits.append(x)
        else:
            mat = x[:no_of_splits-1]
            extra = x[no_of_splits-1:]
            text = ''.join(extra)
            mat.append(text)
            val_splits.append(mat)
    
    df['KeySplits'] = key_splits
    df['ValueSplits'] = val_splits
    
    df2 = pd.DataFrame(df.KeySplits.values.tolist(),index= df.index).add_prefix('Key_')
    df3 = pd.DataFrame(df.ValueSplits.values.tolist(),index= df.index).add_prefix('Val_')
    
    df_final = pd.concat([df,df2,df3],axis = 1)
    
    df_final_copy = df_final
    kv = []    
    for i in range(no_of_splits):
        df_final_copy['Key_'+str(i)] = df_final_copy['Key_'+str(i)].apply(lambda x: _clean_key_column(x))
        df_final_copy['Val_'+str(i)] = df_final_copy['Val_'+str(i)].apply(lambda x: _clean_val_column(x))
        kv.append('Key_'+str(i))
        kv.append('Val_'+str(i))
    
    cols = ['LineId', 'Date', 'Timestamp', 'EventCode', 'MessageType', 'Action',
             'Delim', 'Content', 'EventId', 'EventTemplate']
    cols.extend(kv)
    
    df_view = df_final_copy[cols]
    
    df_view.to_csv(output_path)