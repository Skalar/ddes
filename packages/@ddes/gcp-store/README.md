# @ddes/gcp-store

> GCP powered EventStore, SnapshotStore and MetaStore implementations for DDES, a framework for distributed Event Sourcing & CQRS.

## [API Docs](https://ddes.io/docs/)

## [DDES website](https://ddes.io)

## Prequisites

### Local development

- [Datastore emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator)

### Live development

- Make sure [CLOUD SDK](https://cloud.google.com/sdk/) is installed
- Follow the getting started guide [here](https://cloud.google.com/datastore/docs/activate)
- Authentication for Google Cloud, see [here](https://cloud.google.com/docs/authentication/getting-started)
- Indexes are created and `serving`, see below:

#### Create indexes

To create indexes, first make sure the current project is correct:

To see what project is active, run:

```
gcloud config list project --format "value(core.project)"
```

To change current project (replace)

```
gcloud config set project project-id
```

To upload index:

```
gcloud datastore create-indexes lib/utils/gcp-index-config.yaml
```

Wait for indexes to have status `serving`. You can see the status [here](https://console.cloud.google.com/datastore/indexes)
