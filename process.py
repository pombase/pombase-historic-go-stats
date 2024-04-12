#!/usr/bin/env python3

import polars as pl
import os
import io
import re
import sys
import matplotlib.pyplot as plt
import hvplot.polars
import seaborn as sns

hvplot.extension('matplotlib')

gafs = os.listdir('data')

gafs.sort()

gaf_pattern = re.compile('^(\d\d\d\d-\d\d-\d\d).*');

evidence_codes = [
    'EXP', 'IC', 'IDA', 'IEA', 'IEP', 'IGC', 'IGI', 'IKR',
    'IMP', 'IPI', 'ISM', 'ISO', 'ISS', 'NAS', 'ND', 'RCA', 'TAS'
]

ev_code_groups = {
    'Manual': [
        'EXP', 'IC', 'IDA', 'IEP', 'IGC', 'IGI', 'IKR',
        'IMP', 'IPI', 'ISM', 'ISO', 'ISS', 'NAS', 'ND', 'RCA', 'TAS'
    ],
    'Electronic annotation': [
        'IEA',
    ],
}

groups_by_code = {}

for name, codes in ev_code_groups.items():
    for code in codes:
        groups_by_code[code] = name

plot_columns = [ 'date', 'IEA', 'IEP', 'IKR', 'RCA', 'NAS', 'TAS' ]

seen_dates = {}

def process_one_file(gaf_file):
    match = gaf_pattern.match(gaf_file)
    if match is None:
        print(f'no match: {gaf_file}')
        sys.exit(1)

    date = match.group(1)

#    if date in seen_dates:
#        print(f'seen date already: {date}')
#        return None
#    else:
#        seen_dates[date] = True

    cols = [
        'db', 'db_object_id', 'db_object_symbol', 'qualifier', 'go_id',
        'db_reference', 'evidence_code', 'with_or_from', 'aspect',
        'db_object_name', 'db_object_synonym', 'db_object_type', 'taxon'
    ]

    df = pl.scan_csv(f"data/{gaf_file}", separator="\t",
                     ignore_errors=True, has_header=False,
                     new_columns=cols, comment_prefix="!").select('evidence_code')

    count_df = df.group_by('evidence_code').len(name='count')

    new_df_data = { 'date': date }

    for ev_code, count in count_df.collect().iter_rows():
        if ev_code != '***':
            new_df_data[ev_code] = count

    new_df = pl.DataFrame(new_df_data)

    return new_df

all_df = process_one_file(gafs[0])

for gaf in gafs:
    count_df = process_one_file(gaf)

    if count_df is None:
        continue

    all_df = pl.concat([all_df, count_df], how='align').fill_null(value=0)

all_df = (all_df.with_columns(pl.col('date').str.to_date("%Y-%m-%d"))
          .select(['date'] + evidence_codes))

all_df = all_df.group_by('date').sum().sort('date')

all_df.write_csv('table.tsv', separator="\t")

## average per year:
#pandas_df = (all_df
##             .upsample(time_column='date', every='1w')
##             .sort('date')
##             .interpolate()
#             .with_columns(pl.col('date').dt.year())
#             .group_by('date')
#             .mean()
#             .select(plot_columns)
#             .sort('date')).to_pandas().set_index('date')

pandas_df = (all_df.with_columns(pl.col('date').dt.to_string("%Y-%m-%d"))
#             .select(plot_columns)
             .to_pandas().set_index('date'))



plt.figure().clear()
sns.set(rc={"figure.figsize":(36, 15)})
plt.rcParams["savefig.dpi"] = 15
sns.set_theme(style="whitegrid")
sns.set_palette("deep")

plot = pandas_df.plot(kind='bar',stacked=True, width=0.9, edgecolor='none')

plot.get_figure().savefig('figure.svg', format="svg",
                          pad_inches=0.2, bbox_inches="tight")
