# Local Mode

In this deployment, our resource's metadata will be stored locally on your file system and transformations will be run locally on your machine. This allows you to define and manage your local data sources, transformations, features, and training sets. It's also a great way to get a feel for the API without having to deploy Kubernetes.

It stores all your metadata and transformations in a SQLite database that's created in a \~/.featureform directory.

Local mode come built into the featureform PyPi package.

### Requirements

- Python 3.7+

{% hint style="info" %}
Incompatible with M1 Macs on Python3.10 due to a bug with GRPC
{% endhint %}

```bash
pip install featureform
```

You can then create a special provider, called a local provider.

```
import featureform as ff

local = ff.register_local()
```

Instances of ServingClient should be replaced with ServingLocalClient. Both implement the same API otherwise.

## Current Limitations

### No mix-and-matching local and deployed

You cannot use features both locally and in the deployed mode currently. Our goal is to combine the two so they can be used seamlessly together. In that workflow you'd be able to experiment locally, and push to a local repository. You can also push to a remote repository when resources are ready to be deployed.
