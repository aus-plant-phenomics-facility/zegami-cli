# APPF fork of zegami-cli, including automated collection creation
A Command Line Interface for [Zegami](https://www.zegami.com).


# Background
Some small changes are required to the upstream https://github.com/zegami/zegami-cli to work with data from the LemnaTec system at The Plant Accelerator.

Notably:
* Support for images without file extensions. (Including default MIME as `png`).
* Support for pgsql.

If these changes were to be made upstream, this project could be modified to just include the `appf-collection-builder` code.

# Usage
## Download and build

1. `git clone https://github.com/aus-plant-phenomics-facility/zegami-cli`
2. `cd zegami-cli`
3. `docker build . -t zegami-cli`

Create and configure `.env` file.
```
USERNAME=zegami_user@domain.com
PASSWORD=zegami_password
```

## Interactive Mode (Supports upload from CSVs with filepaths)
`docker run --network="host" -v /zegami/collections/:/export_images -v /home/zegami/ftp-2019/:/prod_images -v /home/zegami/data/:/data_source -v /home/ubuntu/zegami-cli/projects/:/projects --env-file /home/ubuntu/zegami-cli/.env -it zegami-cli`

## Auto (Automatically upload all production database data)
`docker run --network="host" -v /home/zegami/ftp-2019/:/prod_images -v /home/ubuntu/zegami-cli/projects/:/projects --env-file /home/ubuntu/zegami-cli/.env zegami-cli auto`

# Caveat
This project is designed to be open but major effort has not been invested to make it work with any LemnaTec environment. It is likely that some changes would be required to have it work within your environment. Please contact george.sainsbury@adelaide.edu.au for enquiries.