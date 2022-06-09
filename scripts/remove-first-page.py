import os, sys
import pikepdf
from progress.bar import Bar

dir = sys.argv[1]
newdir = dir + "_new"
os.makedirs(newdir, exist_ok=True)
filenames = os.listdir(dir)

progressbar = Bar("Removing first page...", bar_prefix=' [', bar_suffix='] ', empty_fill='_',
                  fill='▓', suffix='%(index)d/%(max)d',
                  max=len(filenames))

for i, file_name in enumerate(filenames):
    progressbar.goto(i)
    if not os.path.exists(os.path.join(newdir, file_name)):
        file_path = os.path.join(dir, file_name)
        try:
            with pikepdf.open(file_path) as pdf:
                del pdf.pages[0]
                pdf.save(os.path.join(newdir, file_name))
        except Exception as err:
            print(str(err))
progressbar.finish()
