import os, sys
import json, csv
import re

import pandas

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from src.domain_terminology_extraction import TextRank4Keyword, get_words_from_file, get_replace_dict_from_file
import pandas as pd

# Command line arguments
corpus_dir = sys.argv[1]
period_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1
replace_terms_file_path = sys.argv[3] if len(sys.argv) > 3 else None

if not os.path.isdir(corpus_dir):
    raise ValueError(f'Not a directory: {corpus_dir}')
if replace_terms_file_path and not os.path.isfile(replace_terms_file_path):
    raise ValueError(f'Not a file: {replace_terms_file_path}')

# replace terms
replace_terms = get_replace_dict_from_file(replace_terms_file_path) if replace_terms_file_path else None

# metadata
md = pd.read_csv("data\jls.csv")

# Partition the corpus
corpus_files = os.listdir(corpus_dir)
corpus_files.sort()
years_files = {}
print(f"Processing {len(corpus_files)} documents...")

for file_name in corpus_files:
    doi = file_name.strip(".txt").replace("_", "/", 1)
    try:
        pubyear = int(md.loc[md['DOI'] == doi]['PubYear'])
    except:
        p = re.compile(r"\.([0-9]{4})\.")
        pubyear = int(p.search(doi).groups()[0])
        if not pubyear:
            print(f"Cannot determine year for {doi}")
            continue
    if pubyear not in years_files.keys():
        years_files[pubyear] = [file_name]
    else:
        years_files[pubyear].append(file_name)

years = list(years_files.keys())
years.sort()
year_min, year_max = min(years), max(years)

# output dir
output_dir_name = os.path.basename(corpus_dir) + f"_{year_min}-{year_max}_" + (str(period_size).zfill(2))
output_dir_path = os.path.join(os.path.dirname(corpus_dir), output_dir_name)
os.makedirs(output_dir_path, exist_ok=True)

period_start = None
period_list = []
for year in range(year_min, year_max):
    if period_start is None:
        num_docs = 0
        words = []
        period_start = year
        period_end = year + (period_size - 1)
        period = f"{str(period_start)}-{str(period_end)}" if period_start != period_end else str(period_start)
        period_list.append(period)
        # determine path of outputfile and skip computation if file exists
        output_file_path = os.path.join(output_dir_path, f"{period}.json")
        if os.path.isfile(output_file_path):
            continue
    for file_name in years_files[year]:
        file_path = os.path.join(corpus_dir, file_name)
        words.extend(get_words_from_file(file_path, replace_terms=replace_terms))
        num_docs += 1
    if year == period_end:
        # process period
        print(f"Processing {period} ({num_docs} document(s))...")
        text = ' '.join(words)
        tr4w = TextRank4Keyword()
        tr4w.analyze(text)
        kw_weights = tr4w.get_weights()
        with open(output_file_path, "w") as f:
            json.dump(kw_weights, f)
        # start a new period
        period_start = None

# create a grid of keywords, periods, and weights
all_kw_weights = {}
for period in period_list:
    file_name = os.path.join(output_dir_path, f"{period}.json")
    with open(file_name) as f:
        kw_weights = json.load(f)
        for kw, weight in kw_weights.items():
            if kw in all_kw_weights.keys():
                all_kw_weights[kw][period] = weight
            all_kw_weights[kw] = {period:weight}



