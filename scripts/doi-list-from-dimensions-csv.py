import sys, csv

input_file = sys.argv[1]

with open(input_file) as csvfile:
    reader = csv.DictReader(csvfile)
    doi_list = [row['DOI'] for row in reader]

with open("data/doi-list.txt", "w") as doi_file:
    doi_file.write("\n".join(doi_list))
