import sys, os

dir_path = sys.argv[1]
if not os.path.isdir(dir_path):
    raise ValueError(f"{dir_path} is not a valid directory")

ext = sys.argv[2]
if not ext:
    raise ValueError("Please provide an extension name as second argument")

if not ext.startswith("."):
    ext = "." + ext

strip_extensions = [
    ".pdf",
    ".pdfa",
    ".txt",
    ".txtUnstructured",
    ".pdftotext",
    ".docx"
]
for file_name in os.listdir(dir_path):
    new_file_name = file_name
    for strip_ext in strip_extensions:
        new_file_name = new_file_name.strip(strip_ext)
    os.rename(os.path.join(dir_path, file_name), os.path.join(dir_path, new_file_name + ext))



