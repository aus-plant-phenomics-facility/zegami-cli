# zegami-cli
A Command Line Interface for [Zegami](https://www.zegami.com).

Zegami is a visual data exploration tool that makes the analysis of large collections of image rich information quick and simple.

The Zegami cli relies on a combination of yaml files and arguments.

The first step is to create a collection

# Installation
```
pip3 install zegami-cli[sql]
```

# Commands

## Login
The login command promtps for username and password which is then used to retrieve a long-lived API token which can be used for subsequent requests. The token is stored in a file in the currenet users data directory.
Once retrieved all subsequent commands will use the stored token, unless it is specifically overridden with the `--token` option
```
zeg login
```

## Get a collection
Get the details of a collection.
If the `collection id` is excluded then all collections will be listed.
```
zeg get collections [collection id] --project [Project Id]
```

## Create a collection
Create a collection using a combined dataset and imageset config.
```
zeg create collections --project [Project Id] --config [path to configuration yaml]
```

Project id, or workspace id, can be found in the url of a collection or collection listing page. For example:

https://zegami.com/mycollections/66xtfqsk

In the case of this workspace, it's `66xtfqsk`.

The following config properties are supported for file based imageset and datasets.

```
# The name of the collection
name: file based
description: an example collection with a file based imageset and dataset
# The type of data set. For now this needs to be set to 'file'
dataset_type: file
# Config for the file data set type
imageset_type: file
# Config for the file image set type
file_config:
# Whether to recursively scan any directories. (optional)
    recursive: True
# If provided, the mime-type to use when uploading images. (optional)
    mime_type: image/jpeg
# Path to the dataset file
    path: path/to/file/mydata.csv
# A collection of paths to image files. Paths can be to both images and directories
    paths:
        - an_image.jpg
        - a/directory/path
# Name of the column in the dataset that contains the image name. (optional)
dataset_column: image_name
```
If dataset_column is not provided, the backend will automatically select the column with the closest match.

When providing a `mime_type` property, all files in directories will be uploaded regardless of extension.

If you are creating a url based imageset with a data file use these properties.

The dataset_column property is used to set the column where the url is stored. You will need to include the full image url e.g. https://zegami.com/wp-content/uploads/2018/01/weatherall.svg

```
# The name of the collection
name: url based
# The description of the collection
description: an example collection with a file based dataset where images are to be downloaded from urls
# The type of image set. for now this needs to be set to 'url'
imageset_type: url
# Name of the column in the dataset that contains the image url
dataset_column: image_name
# Url pattern - python format string where {} is the name of the image name (from data file)
url_template: https://example.com/images/{}?accesscode=abc3e20423423497
dataset_type: file
# Config for the file data set type
file_config:
# Path to the dataset file
    path: path/to/file/mydata.csv
```

If you are creating an imageset on Azure from a private azure bucket with a local file do as follows:

```
# The name of the collection
name: azure bucket based
# The description of the collection
description: an example collection with a file based dataset where images are to be downloaded from an azure bucket
dataset_type: file
# Config for the file data set type
file_config:
# Path to the dataset file
    path: path/to/file/mydata.csv
# The type of image set. for now this needs to be set to 'url'
imageset_type: azure_storage_container
# Name of the container
container_name: my_azure_blobs
# Name of the column in the dataset that contains the image url
dataset_column: image_name

# Note that the storage account connection string should also be made available via environment variable AZURE_STORAGE_CONNECTION_STRING
```

If you are using SQL data see below for config

## Update a collection
Update a collection - *coming soon*.

## Delete a collection
Delete a collection
```
zeg delete collections [collection id] --project [Project Id]
```

## Publish a collection
```
zeg publish collection [collection id] --project [Project Id] --config [path to configuration yaml]
```
Similarly to the workspace id, the collection id can be found in the url for a given collection. For instance:

https://zegami.com/collections/public-5df0d8c40812cf0001e99945?pan=FILTERS_PANEL&view=grid&info=true

This url is pointing to a collection with a collection id which is 5df0d8c40812cf0001e99945.

The config `yaml` file is used to specify additional configuration for the collection publish.
```
# The type of update. For now this needs to be set to 'publish'
update_type: publish
# Config for the publish update type
publish_config:
# Flag to indicate if the collection should be published or unpublished
    publish: true
# The id of the project to publish to
    destination_project: public
```

## Get a data set
Get a data set
```
zeg get dataset [dataset id] --project [Project Id]
```
Dataset Ids can be found in the collection information, obtained by running:
```
zeg get collections <collection id> --project <project id>
```
From here `upload_dataset_id` can be obtained. This identifies the dataset that represents the data as it was uploaded. Whereas `dataset_id` identifies the processed dataset delivered to the viewer.

## Update a data set
Update an existing data set with new data.

Note that when using against a collection the dataset id used should be the upload_dataset_id. This is different from the below imageset update which requires the dataset identifier known as dataset_id from the collection.
```
zeg update dataset [dataset id] --project [Project Id] --config [path to configuration yaml]
```

The config `yaml` file is used to specify additional configuration for the data set update. There are *two* supported `dataset_type` supported.

### File
The `file` type is used to update a data set with a file. It can be set up to either specify the fully qualified path to a `.csv.`, `.tsv` or `.xlsx` file to upload using the `path` property *or* the `directory` property can be used to upload the latest file in a directory location.
```
# The type of data set. For now this needs to be set to 'file'
dataset_type: file
# Config for the file data set type
file_config:
# Path to the dataset file
    path: path/to/file/mydata.csv
# Or path to a directory that contains data files.
# Only the latest file that matches the accepted extensions (.csv, .tsv, .xlsx)
# will be uploaded. This is useful for creating collections based on
# automated exports from a system, like log files.
    directory:
```

### SQL
The `sql` type is used to update a data set based on an `SQL` query.
Uses SQLAlchemy to connect to the database. See http://docs.sqlalchemy.org/en/latest/core/engines.html and https://www.connectionstrings.com/ for the correct connection string format.

```
# The type of data set. For now this needs to be set to 'file'
dataset_type: sql
# Config for the sql data set type
sql_config:
# The connection string.
    connection:
# SQL query
    query:
```

### PostgreSQL - tested on Linux and windows, up to Python v3.8
Pre-requisites :

1. Standard requirements - code editor, pip package manager, python 3.8.

2. Make sure Zegami CLI latest is installed
```
pip install zegami-cli[sql] --upgrade --no-cache-dir
```
_Note: --no-cache-dir avoids some errors upon install_

Test the install with the login command, which prompts for username and password. This is then used to retrieve a long-lived API token which can be used for subsequent requests. The token is stored in a file in the current users data directory.
Once retrieved all subsequent commands will use the stored token, unless it is specifically overridden with the `--token` option
```
zeg login
```

3. Install pre-requirements for PostgreSQL connection

Psycopg2 - https://pypi.org/project/psycopg2/ , http://initd.org/psycopg/
```
pip install python-psycopg2
```

_libpq-dev was required for linux, not windows_
libpq-dev - https://pypi.org/project/libpq-dev/ , https://github.com/ncbi/python-libpq-dev
```
sudo apt-get install libpq-dev
```

Once these are installed you will need to create a YAML file with the correct connection strings.

*Connection String Example:*
```
# The type of data set. For now this needs to be set to 'file'
dataset_type: sql
# Config for the sql data set type
sql_config:
# The connection string.
    connection: "postgresql://postgres:myPassword@localhost:5432/postgres?sslmode=disable"
# SQL query
    query: select * from XYZ
```
_Note: Connections strings must have indentation by "connection" and "query"_

If you have already created a collection we can run the update command as above
e.g. zeg update dataset upload_dataset_id --project projectID --config root/psqlconstring.yaml

If successful the following message will appear:
```
=========================================
update dataset with result:
-----------------------------------------
id: datasetID
name: Schema dataset for postgresql test
source:
  blob_id: blobID
  dataset_id: datasetID
  upload:
    name: zeg-datasetiop9cbtn.csv

=========================================
```

Useful links:
https://www.npgsql.org/doc/connection-string-parameters.html
https://www.connectionstrings.com/postgresql/ (Standard)
https://docs.sqlalchemy.org/en/13/core/engines.html#postgresql (Specifies pre-reqs for connection)




## Delete a data set
Delete a data set - *coming soon*.
```
zeg delete dataset [dataset id] --project [Project Id]
```

## Get an image set
Get an image set - *coming soon*.
```
zeg get imageset [imageset id] --project [Project Id]
```

## Update an image set
Update an image set with new images.
```
zeg update imageset [imageset id] --project [Project Id] --config [path to configuration yaml]
```

The config `yaml` file is used to specify additional configuration for the image set update. Note that an imageset can only be changed before images are added to it.

### File imageset

The `paths` property is used to specify the location of images to upload and can include both images and directories.


```
# The type of image set. for now this needs to be set to 'file'
imageset_type: file
# Config for the file image set type
file_config:
# A collection of paths. Paths can be to both images and directories
    paths:
        - an_image.jpg
        - a/directory/path
# Unique identifier of the collection
collection_id: 5ad3a99b75f3b30001732f36
# Unique identifier of the collection data set (get this from dataset_id)
dataset_id: 5ad3a99b75f3b30001732f36
# Name of the column in the dataset that contains the image name
dataset_column: image_name
```

### URL imageset

The dataset_column property is used to set the column where the url is stored. You will need to include the full image url e.g. https://zegami.com/wp-content/uploads/2018/01/weatherall.svg

```
# The type of image set. for now this needs to be set to 'url'
imageset_type: url
# Unique identifier of the collection
collection_id: 5ad3a99b75f3b30001732f36
# Unique identifier of the collection data set
dataset_id: 5ad3a99b75f3b30001732f36
# Name of the column in the dataset that contains the image url
dataset_column: image_name
# Url pattern - python format string where {} is the name of the image name (from data file)
url_template: https://example.com/images/{}?accesscode=abc3e20423423497
```

### Azure storage imageset

```
# The type of image set.
imageset_type: azure_storage_container
# Name of the container
container_name: my_azure_blobs
# Unique identifier of the collection
collection_id: 5ad3a99b75f3b30001732f36
# Unique identifier of the collection data set
dataset_id: 5ad3a99b75f3b30001732f36
# Name of the column in the dataset that contains the image url
dataset_column: image_name

# Note that the storage account connection string should also be made available via environment variable AZURE_STORAGE_CONNECTION_STRING
```

## Delete an image set
Delete an image set - *coming soon*.
```
zeg delete imageset [imageset id] --project [Project Id]
```


# Developer

## Tests
Setup tests:
```
pip install -r requirements/test.txt
```

Run tests:
```
python3 -m unittest discover .
```
