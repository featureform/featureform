# Github Actions Workflows

Github Actions Workflow Definitions

## Files
- *ci.yml*: Workflow for Embeddinghub
- *client-ci.yml*: Workflow that generates documentation from Python doc strings and 
pushes it to Github pages. 
- *dashboard-workflow*: Runs dashboard tests
- *deploy-helm*: Deploys the Helm charts and Docker containers to production. Takes a version
number as an argument and can be run manually
- *deploy-python*: Deploys the Python package to Pypi. Takes a version number as an argument
and can be run manually
- *e2e.yml*: Runs end-to-end tests using minikube. Specifically it runs the minikube quickstart to
ensure it's working properly. 
- *link-check.yml*: Runs link checks to detect broken links in the README's, 
docs, and websites
- *linter.yml*: Runs a linter on the golang files. If not updated properly, it updates 
and commits the reformatted files. 
- *testing.yml*: Runs all other integration and unit tests for the repo