### Historical GO evidence codes

Every two months, add new GO-style GAF files to `raw_data/`
For example copy:
  https://curation.pombase.org/dumps/releases/pombase-2024-07-01/misc/go_style_gaf.tsv
to:
```
raw_data/2024-07-01-go_style.gaf
```

Other GAF files can be added to `extra_pombase_data/` to keep to
complete history of changes

To re-generate figure.svg and table.tsv:

```sh
   uv run ./process.py
```

Then copy `figure.svg` to `website/src/assets/pombase_history_go_ev_codes.svg`
and:
```
git add figure.svg table.parquet table.tsv
git commit
git push
```
