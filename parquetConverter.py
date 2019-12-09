# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import pandas as pd
import pyarrow.parquet as pq

print('Appending files')
files = ['part-00001-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet','part-00002-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet',
         'part-00003-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet','part-00004-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet',
         'part-00005-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet','part-00006-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet',
         'part-00007-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet','part-00008-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet',
         'part-00009-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet','part-00010-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet',
         'part-00011-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet','part-00012-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet',
         'part-00013-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet','part-00014-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet',
         'part-00015-d97e9529-a3c6-4c51-a47c-74c5a34f3abe-c000.snappy.parquet']

for f in files:
    print 'Reading ',f
    table = pq.read_table(f)
    df = table.to_pandas()
    with open('csv_file.csv','a') as file:
        df.to_csv(file, header=False, index=False)
        print f, 'appended to csv_file.csv'