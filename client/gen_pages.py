import mkdocs_gen_files
import os

excluded_files = [
    '__pycache__',
    'proto',
    '__init__.py',
    '__main__.py',
    'cli.py',
    'dashboard_metadata.py',
    'format.py',
    'get.py',
    'list.py',
    'local.py',
    'resources.py',
    'serving_test.py',
    'serving.py',
    'sqlite_metadata.py',
    'type_objects.py',
    'register.py',
    'dashboard',
    'get_local.py',
    'get_test.py',
    'list_local.py',
    'tls.py'
]

for filename in os.listdir("./src/featureform"):
    if filename in excluded_files:
        continue
    if 'test' in filename:
        continue

    file = filename.split(".")[0]
    mdFile = f"{file}.md"

    with mkdocs_gen_files.open(mdFile, "w") as f:
        print(f":::src.featureform.{file}", file=f)

    mkdocs_gen_files.set_edit_path(mdFile, "gen_pages.py")