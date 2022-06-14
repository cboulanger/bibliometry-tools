import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from src.domain_terminology_extraction import TextRank4Keyword, get_words_from_file, get_replace_dict_from_file
import pandas as pd
import json, csv
import re
import inflect

infl = inflect.engine()

# Command line arguments
corpus_dir = sys.argv[1]
period_size = int(sys.argv[2]) if len(sys.argv) > 2 else 1

if not os.path.isdir(corpus_dir):
    raise ValueError(f'Not a directory: {corpus_dir}')

# corpus dir paths
corpus_dir_basename = os.path.basename(corpus_dir)
corpus_dir_dirname = os.path.dirname(corpus_dir)
corpus_dir_prefix = corpus_dir_basename[:corpus_dir_basename.find("-")]
replace_terms_file_path = os.path.join(corpus_dir_dirname, corpus_dir_prefix + "-replace-terms.csv")

# replace terms
if not os.path.isfile(replace_terms_file_path):
    raise FileNotFoundError(f"Missing term replacement file {replace_terms_file_path}")
replace_terms = get_replace_dict_from_file(replace_terms_file_path)

# metadata
doi_metadata_file = os.path.join(corpus_dir_dirname, corpus_dir_prefix + "-doi.csv")
if not os.path.isfile(doi_metadata_file):
    raise FileNotFoundError(f"Metadata file {doi_metadata_file} does not exist")
md = pd.read_csv(doi_metadata_file)

# Partition the corpus
corpus_files = os.listdir(corpus_dir)
corpus_files.sort()
years_files = {}

print("Looking up publication years from DOI...")
for file_name in corpus_files:
    doi = file_name.replace("_", "/", 1).strip(".txt")
    #print(doi)
    try:
        pubyear = md.loc[md['DOI'] == doi]['PubYear']
        pubyear = int(pubyear)
    except TypeError:
        p = re.compile(r"\D((19|20)\d{2})\D")
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
output_dir_name = f"{corpus_dir_basename}_{year_min}-{year_max}_" + (str(period_size).zfill(2))
output_dir_path = os.path.join(corpus_dir_dirname, output_dir_name)
os.makedirs(output_dir_path, exist_ok=True)

print(f"Processing {len(corpus_files)} documents into {output_dir_name} ...")

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
    if year not in years_files.keys():
        continue
    for file_name in years_files[year]:
        file_path = os.path.join(corpus_dir, file_name)
        words.extend(get_words_from_file(file_path, replace_terms=replace_terms))
        num_docs += 1
    if year == period_end:
        # start a new period
        period_start = None
        # skip already processed periods
        if os.path.isfile(output_file_path):
            print(f"- {period} ({num_docs} {infl.plural('document', num_docs)}) already processed...")
            continue
        break
        # process period
        print(f"- Processing {period} ({num_docs} {infl.plural('document', num_docs)})...")
        text = ' '.join(words)
        tr4w = TextRank4Keyword()
        tr4w.analyze(text)
        kw_weights = tr4w.get_weights()
        with open(output_file_path, "w") as f:
            json.dump(kw_weights, f)

# create a grid of keywords, periods, and weights
all_kw_weights = {}
for period in period_list:
    file_path = os.path.join(output_dir_path, f"{period}.json")
    if not os.path.isfile(file_path):
        print(f"Skipping non-existent data for {period}..")
        continue
    with open(file_path) as f:
        kw_weights = json.load(f)
        ignore_list = replace_terms.keys()
        for kw, weight in kw_weights.items():
            if kw in ignore_list:
                continue
            if kw in all_kw_weights.keys():
                all_kw_weights[kw][period] = weight
            else:
                all_kw_weights[kw] = {period: weight}

# create dataframe keywords x periods, containing the weights and save to filesystem
rows = all_kw_weights.values()
keywords = all_kw_weights.keys()
df = pd.DataFrame(rows, index=keywords)
pickle_path = os.path.join(corpus_dir_dirname, corpus_dir_basename + ".pkl")
df.to_pickle(pickle_path)
print(f"Dataframe containing the weights per keyword and period has been saved to {pickle_path}")
