import sys, os,shutil,csv, regex as re
from langdetect import detect

dir_path = sys.argv[1]
if not os.path.isdir(dir_path):
    raise ValueError(f"{dir_path} is not a valid directory")

dir_basename = os.path.dirname(dir_path)
dir_prefix = dir_basename[:dir_basename.find("-")]

# csv file first column is term regex, second optional column is file name regex
exclude_doc_terms_path = os.path.join(dir_basename, dir_prefix + "-exclude-doc-terms.csv")
exclude_doc_terms_list = []
if os.path.isfile(exclude_doc_terms_path):
    with open(exclude_doc_terms_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if row is list and row[0]:
                if row[0].startswith("#"): continue # allow comment lines
                if row[0] == "" and row[1] == "": continue # skip empty values
                exclude_doc_terms_list.append(row)

for file_name in os.listdir(dir_path):
    file_path = os.path.join(dir_path, file_name)
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()
    # exclude doc based on content and/or file name?
    for term_re, filename_re in exclude_doc_terms_list:
        if filename_re == "" or re.match(filename_re, file_name):
            if term_re == "" or re.match(term_re, text):
                continue
    # sort by language
    lang = detect(text)
    lang_dir = os.path.join(dir_basename, dir_path + "-" + lang)
    os.makedirs(lang_dir, exist_ok=True)
    shutil.copy(file_path, lang_dir)

