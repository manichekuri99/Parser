import pandas as pd 
import csv 

def concat_templates(window,column,dataframe,outputpath):
    f = open(outputpath, 'w')
    writer = csv.writer(f)
    n = 0
    data1 = {
    'Template': []
    }
    df1 = pd.DataFrame(data1)
    while n<len(dataframe):
        dat = ""
        for i in range(window):
            row = dataframe.iloc[i]
            text = row.loc[column].strip(" ")
            dat = dat+"_"+text
            n= n+1
        
        ls = [dat[1:]]    
        row = pd.Series(ls, index=df1.columns)
        df1 = df1.append(row, ignore_index=True)
        
    df1.to_csv("output.csv")





df = pd.read_csv("./Spell_result/bootstrap-armada.log_structured.csv")

# print("specify window size")
# window_size = int(input())

# print("column to concat")
# column = input()


concat_templates(20,"EventTemplate",df,"output.csv")