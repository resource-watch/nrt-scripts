# eeUtil

Wrapper for easier data management with Earth Engine python sdk

Requires account with access to Google Cloud Storage and Earth Engine.

```
import eeUtil as eu

# initialize from environment variables
eu.init(bucket='mybucket')

# create image collection
eu.createFolder('mycollection', imageCollection=True)

# upload image to collection
eu.upload('image.tif', 'mycollection/myasset')
eu.setAcl('mycollection/myasset', 'public')
eu.ls('mycollection')
```

### Nice things?

- More consistent python bindings
- GEE paths not starting with `/` or `users/` are relative to your user root folder (`users/<username>`)
- Upload atomatically stages files via Google Cloud Storage

### Usage

eeUtil defaults to reading from credentials saved by `gcloud auth` for Google Cloud Storage and `earthengine authenticate` for Earth Engine. In your script, initialize these credentials with `eeUtil.init()`

Alternatively credentials can be provided directly to `eeUtil.init()` or read from environment.

```
eeUtil.init([service_account=], [credential_path=], [project=], [bucket=])
```

 - `service_account` Service account name. If not specficed, reads defaulds from `earthengine authenticate`. For more information on GEE service accounts, see: https://developers.google.com/earth-engine/service_account
 - `credential_path` Path to json file containing private key. Required for service accounts.
 - `project` GCS project containing bucket. Required if account has access to multiple projects.
 - `bucket` Storage bucket for staging assets for ingestion. Will create new bucket if none provided.

```
# environment variables
export GEE_SERVICE_ACCOUNT=<my-account@gmail.com>
export GOOGLE_APPLICATION_CREDENTIALS=<path/to/credentials.json>
export GCS_PROJECT=<my-project>
export GEE_STAGING_BUCKET=<my-bucket>
```




