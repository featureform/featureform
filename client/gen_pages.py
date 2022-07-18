import mkdocs_gen_files
import os

excluded_files = ['__pycache__', 'proto', '__init__.py', '__main__.py', 'cli.py', 'client_test.py', 'resource_client_test.py', 'dashboard_metadata.py', 'format.py', 'get.py', 'list.py', 'local_test.py', 'local.py', 'register_test.py', 'resources_test.py', 'resources.py', 'serving_test.py', 'serving.py', 'sqlite_metadata.py', 'type_objects.py', 'register.py']

for filename in os.listdir("./src/featureform"):
    if filename in excluded_files:
        continue

    file = filename.split(".")[0]
    mdFile = f"{file}.md"

    with mkdocs_gen_files.open(mdFile, "w") as f:
        print(f":::src.featureform.{file}", file=f)

    mkdocs_gen_files.set_edit_path(mdFile, "gen_pages.py")