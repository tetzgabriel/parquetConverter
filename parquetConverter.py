# encoding=utf8
import pandas as pd
import pyarrow.parquet as pq
import sys
reload(sys)
sys.setdefaultencoding('utf8')

print('Appending files')
files = ['part-00001.parquet','part-00002.parquet',
         'part-00003.parquet','part-00004.parquet',
         'part-00005.parquet','part-00006.parquet',
         'part-00007.parquet','part-00008.parquet',
         'part-00009.parquet','part-00010.parquet',
         'part-00011.parquet','part-00012.parquet',
         'part-00013.parquet','part-00014.parquet',
         'part-00015.parquet']

for f in files:
    print 'Reading ',f
    table = pq.read_table(f)
    df = table.to_pandas()
    with open('csv_file.csv','a') as file:
        df.to_csv(file, header=False, index=False)
        print f, 'appended to csv_file.csv'