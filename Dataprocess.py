import pandas as pd 
import collections as col

DistributedLogLine = col.namedtuple('DistributedLogLine',
                                ['ts', 'text',
                                 'processed', 'proc_dict',
                                 'template', 'templateId', 'template_dict'])


DistributedTemplateLine = col.namedtuple(
    'DistributedTemplateLine', [
        'id', 'template', 'skip_words', 'raw_str'])

def readloglines(df):
    for i in df.index:
        yield DistributedLogLine(
            ts = df.iloc[i,2],
            text = df.iloc[i,7],
            processed = df.iloc[i,7],
            proc_dict=None,
            template=None,
            templateId=None,
            template_dict=None,
        )

def try_parsing_date(text):
    for fmt in ("%H:%M:%S.%f", "%H:%M:%S %f"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')
def readrawlog(f):
    for line in f:
        time = line[11:23]
        t = time.strip(':')
        print(t)
        s=""
        for c in time:
            if c != ':' or c != '.' or c != ' ':
                s = s+c
        tx = line[0:11]+line[23:]
        # t = time.mktime(
        #         datetime.datetime.strptime(
        #             time, "%H:%M:%S.%f").timetuple())
        t = float(s)
        yield DistributedLogLine(
            ts = t,
            text = tx,
            processed = tx,
            proc_dict=None,
            template=None,
            templateId=None,
            template_dict=None,
        )


df = pd.read_csv("./Spell_result/bootstrap-armada.log_structured.csv")


df = open("./Data/bootstrap-armada.log","r")
loglines = readrawlog(df)
row2 = next(loglines, None)
print(row2[1])

# import csv 

# header = ['id', 'template', 'skip_words', 'raw_str']

# f = open('ouput.csv', 'w')

# # create the csv writer
# writer = csv.writer(f)

# writer.writerow(header)

# def writecsv(writer,data):
#     ans = []
#     for i in range(len(data)):
#         data = [data[i].template,data[i].skip_words,data[i].raw_str]
#         ans.append(data)
#     return ans
        