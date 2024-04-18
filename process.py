#!/usr/bin/env python3

# Run with:
#   python3 process.py
# Or:
#   python3 process.py use_groups

import polars as pl
import os
import io
import re
import sys
import matplotlib.pyplot as plt
import hvplot.polars
import seaborn as sns

hvplot.extension('matplotlib')

use_groups = False

if len(sys.argv) > 1 and sys.argv[1] == 'use_groups':
    use_groups = True

gafs = os.listdir('data')

gafs.sort()

gaf_pattern = re.compile('^(\d\d\d\d-\d\d-\d\d).*');

evidence_codes = [
    'IEA', 'EXP', 'IC', 'IDA', 'IEP', 'IGC', 'IGI', 'IKR',
    'HDA', 'HMP', 'IBA', 'ISA',
    'IMP', 'IPI', 'ISM', 'ISO', 'ISS', 'NAS', 'ND', 'RCA', 'TAS'
]

ev_code_groups = {
    'Electronic annotation': [
        'IEA',
    ],
    'Author statements': [
        'TAS', 'IC', 'NAS'
    ],
    'Manual from orthologs': [
        'ISM', 'ISO', 'ISS',
    ],
    'Phylogenetic inference': ['IBA'],
    'Experimental': [
        'EXP',
         'IDA', 'IEP', 'IGC', 'IGI', 'IKR',
        'HDA', 'HMP', 'ISA',
        'IMP', 'IPI',  'ND', 'RCA', 
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
            if date < '2005-01-01':
                ev_code = 'IEA'
            if use_groups:
                group_name = groups_by_code[ev_code]
                if group_name not in new_df_data:
                    new_df_data[group_name] = 0
                new_df_data[group_name] += count
            else:
                new_df_data[ev_code] = count

    new_df = pl.DataFrame(new_df_data)

    return new_df

all_df = process_one_file(gafs[0])

for gaf in gafs:
    count_df = process_one_file(gaf)

    if count_df is None:
        continue

    all_df = pl.concat([all_df, count_df], how='align').fill_null(value=0)

all_df_column_names = ['date'] + evidence_codes

if use_groups:
    all_df_column_names = ['date'] + list(ev_code_groups.keys())

all_df = (all_df.with_columns(pl.col('date').str.to_date("%Y-%m-%d"))
          .select(all_df_column_names))

all_df = all_df.group_by('date').sum().sort('date')

all_df.write_csv('table.tsv', separator="\t")

#print(all_df.with_columns(pl.col('date').dt.to_string("%Y-%m-%d")).head())

#print(all_df.with_columns(pl.col('date').dt.datetime()))


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
sns.set(style="whitegrid", font_scale=0.7)
fig, ax = plt.subplots(dpi=300, figsize=(16, 10))
sns.set_palette("deep")

plot = pandas_df.plot(ax=ax, kind='bar',stacked=True, width=0.8, edgecolor='none')
plot.set_ylabel('Number of annotations', fontsize=22)
plot.set_xlabel(None)
ax.yaxis.set_tick_params(labelsize = 16);
ax.xaxis.set_tick_params(labelsize = 6);

plt.legend(fontsize='18')

fig.savefig('figure.svg', format="svg",
            pad_inches=0.2, bbox_inches="tight")
