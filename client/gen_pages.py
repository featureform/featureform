import mkdocs_gen_files
import os

for filename in os.listdir("./src/featureform"):
    file = filename.split(".")[0]
    mdFile = f"{file}.md"
    if "__" in file:
        continue
    with mkdocs_gen_files.open(mdFile, "w") as f:
        print(f":::src.featureform.{file}", file=f)

    mkdocs_gen_files.set_edit_path(mdFile, "gen_pages.py")