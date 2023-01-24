# Backup and Restore

Featureform can be configured to take periodic snapshots of itself that are backed up
to your specified cloud storage. In case of an incident, this snapshot can be pulled 
and reloaded to restore Featureform to a previous state.

## Enable Snapshotting

### Enable With Helm
Snapshotting is enabled through the helm chart configuration. To enable, you can run:
```shell
helm upgrade featureform featureform/featureform [FLAGS] --set backup.enable=true --set backup.schedule=<schedule>
```

where `<schedule>` is a valid cron schedule. Example: `"0 * * * *"` for every hour on the hour.


### Backup Location
You'll also need to create a Kubernetes secret to provide access to the cloud storage. 

There is an example file located in the [Github Repo](https://github.com/featureform/featureform/tree/main/backup) 
`secret_template.yaml`.

You can use this template to choose one of the cloud providers and add credentials for
that provider. 

Running
```shell
kubectl apply -f secret_template.yaml
```

will create the secret.

## Restore Snapshot

Restoring a snapshot will delete all data currently in Featureform and replace it with 
the data in the snapshot. You can specify to restore from the latest snapshot or
restore from a specific snapshot.

### Restore From Latest
To restore from latest you can download the [Github Repo](https://github.com/featureform/featureform).
```shell
cd backup/restore
```

Edit the `.env-template` file with your cloud provider name and credentials, then
rename to `.env`. A specific snapshot can be used by filling in the `SNAPSHOT_NAME` variable 
in the `.env` file.

To restore, run
```shell
./restore.sh
```
and confirm that the cluster being restored is the correct one. Press `y` to
complete the restore.