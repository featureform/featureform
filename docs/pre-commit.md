# pre-commit

[pre-commit](https://pre-commit.com/) is a tool that allows users to add hooks before you can commit any changes to the repository.

## Install pre-commit
### Using Python
```
pip install pre-commit
```
### Using Homebrew
```
brew install pre-commit
```
<br /> 

## Using pre-commit
1. Inside the root level of the repository, run `pre-commit install`. 
2. Commit a change to the repository. The commit will trigger the hooks to be executed. Upon the passing all the hooks, the commit will be achieved. If any hook fails, the commit will not go through. 

Example of successful message:

```
$ git commit -m "adding pre-commit docs"
Detect secrets...........................................................Passed
[feature/precommit 96fa97c] adding pre-commit docs
 1 file changed, 25 insertions(+)
 create mode 100644 docs/pre-commit.md
```

<br /> 

## Currently Installed Hooks

- **[Yelp's detect-secrets](https://github.com/Yelp/detect-secrets)**: this detects secrets within the repository. For more information including how to ignore false positives, check their GitHub repository.
