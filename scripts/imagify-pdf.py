import sys, os, zlib
import fitz
from progress.bar import Bar
from PIL import Image
#from pikepdf import Pdf, PdfImage, Name

input_dir = sys.argv[1]
output_dir = sys.argv[2]
dpi = 200

filenames = os.listdir(input_dir)
progressbar = Bar("Extracting text...", bar_prefix=' [', bar_suffix='] ', empty_fill='_',
                  fill='▓', suffix='%(index)d/%(max)d',
                  max=len(filenames))

for i, file_name in enumerate(filenames):
    progressbar.goto(i)
    if not file_name.endswith(".pdf"):
        continue
    input_path = os.path.join(input_dir, file_name)
    output_path = os.path.join(output_dir, file_name)
    # convert PDF to images
    doc = fitz.Document(input_path)
    image_paths = []
    for i, page in enumerate(doc.pages()):
        pix = page.get_pixmap(dpi=dpi)
        img_file = os.path.join(output_dir, f"page_{i}.png")
        pix.save(img_file)
        image_paths.append(img_file)
    # convert images to PDF
    images = [Image.open(f) for f in image_paths]
    images[0].save(output_path, "PDF", resolution=dpi, save_all=True, append_images=images[1:])
    # remove images
    for img in image_paths:
        os.remove(img)
    # compress images in PDF, this actually increases the file size!!
    # pdf = Pdf.open(output_path, allow_overwriting_input=True)
    # for page in pdf.pages:
    #     for raw_img in page.images.values():
    #         pdf_img = PdfImage(raw_img)
    #         pillowimage = pdf_img.as_pil_image()
    #         pdf_img.obj.write(zlib.compress(pillowimage.tobytes(), level=9), filter=Name("/FlateDecode"))
    # pdf.save()

progressbar.finish()
