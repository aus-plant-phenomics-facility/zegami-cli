# Copyright 2018 Zegami Ltd

"""Collection commands."""

import concurrent.futures
import os
import uuid

from colorama import Fore, Style
from tqdm import tqdm

from . import (
    config,
    azure_blobs,
    http,
)


MIMES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
    ".dcm": "application/dicom",
    "": "image/png"
}


def get(log, session, args):
    """Get an image set."""
    log('imageset id: {highlight}{id}{reset}',
        highlight=Fore.GREEN,
        id=args.id,
        reset=Style.RESET_ALL)
    log.warn('Get imageset command coming soon.')


def _update_file_imageset(log, session, configuration):
    create_url = "{}imagesets/{}/image_url".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        configuration["id"])
    complete_url = "{}imagesets/{}/images".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        configuration["id"])
    log.debug('POST: {}'.format(create_url))
    log.debug('POST: {}'.format(complete_url))

    # get image paths
    file_config = configuration['file_config']
    # check colleciton id, dataset and join column name

    with concurrent.futures.ThreadPoolExecutor(http.CONCURRENCY) as executor:
        paths = _resolve_paths(file_config['paths'])
        futures = [
            executor.submit(
                _upload_image,
                path,
                session,
                create_url,
                complete_url,
                log,
            ) for path in paths
        ]
        kwargs = {
            'total': len(futures),
            'unit': 'image',
            'unit_scale': True,
            'leave': True
        }
        for f in tqdm(concurrent.futures.as_completed(futures), **kwargs):
            pass


def _update_join_dataset(
        log, configuration, dataset_id, dataset_column, session,
        collection_id):
    dataset_create_url = "{}datasets/".format(
        http.get_api_url(configuration["url"], configuration["project"])
    )
    log.debug('POST: {}'.format(dataset_create_url))

    join_data = {
        'name': 'join dataset',
        'source': {
            'imageset_id': configuration["id"],
            'dataset_id': dataset_id,
            'imageset_to_dataset': {
                'dataset_column': dataset_column,
            },
        },
    }
    log.debug('POST (join dataset): {}'.format(dataset_create_url))
    # create the join dataset
    join_response = http.post_json(session, dataset_create_url, join_data)
    collection_url = "{}collections/{}".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        collection_id,
    )
    join_id = join_response['dataset']['id']
    # update the collection with the new dataset
    log.debug('GET (collection): {}'.format(collection_url))

    # first need to get the collection object
    collection_response = http.get(session, collection_url)
    collection = collection_response['collection']
    dz_json_join_url = "{}datasets/{}".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        collection["dz_json_dataset_id"]
    )

    # Get existing dz json join dataset (get tilesize etc from this)
    log.debug('GET (dz json): {}'.format(dz_json_join_url))
    dz_json_join = http.get(session, dz_json_join_url)

    # create new dz json based on existing
    for_create = {
        "name": dz_json_join['dataset']["name"],
        "source": dz_json_join['dataset']["source"],
    }
    for_create["source"]["dataset_id"] = join_id

    # Add new dz json dataset, which will become the used one after processing
    log.debug('POST: {}'.format(dataset_create_url))
    dz_json_response = http.post_json(session, dataset_create_url, for_create)

    # TODO wait until processing finished before switching to new dz_json
    # Point collection at new dz json
    collection['imageset_dataset_join_id'] = join_id
    collection['dz_json_dataset_id'] = dz_json_response["dataset"]['id']
    log.debug('PUT: {}'.format(collection_url))
    http.put_json(session, collection_url, collection)


def get_from_dict(data_dict, maplist):
    for k in maplist:
        data_dict = data_dict[k]
    return data_dict


def check_can_update(ims_type, ims):
    features = {
        "file": ["source", "upload"],
        "url": ["source", "transfer", "url"],
        "azure_storage_container": ["source", "transfer", "url"],
    }
    try:
        get_from_dict(ims, features[ims_type])
    except KeyError:
        if len(ims.get("images", [])) != 0:
            raise ValueError(
                "Chosen imageset already has images, cannot change type")


def update(log, session, args):
    configuration = config.parse_args(args, log)
    update_from_dict(log, session, configuration)


def update_from_dict(log, session, configuration):
    """Update an image set."""
    # check for config
    ims_type = configuration["imageset_type"]
    ims_id = configuration["id"]
    ims_url = "{}imagesets/{}".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        ims_id,
    )
    ims = http.get(session, ims_url)["imageset"]
    check_can_update(ims_type, ims)
    if ims_type == "url":
        _update_to_url_imageset(session, configuration, ims_url)
    elif ims_type == "file":
        _update_file_imageset(log, session, configuration)
    elif ims_type == "azure_storage_container":
        if os.environ.get('AZURE_STORAGE_CONNECTION_STRING', None) is None:
            log.error(
                "The AZURE_STORAGE_CONNECTION_STRING environment variable" +
                " must be set in order to create an azure storage collection"
            )
        configuration["url_template"] = azure_blobs.generate_signed_url(
            configuration["container_name"])
        _update_to_url_imageset(session, configuration, ims_url)
    collection_id = configuration['collection_id']
    dataset_id = configuration['dataset_id']
    dataset_column = configuration['dataset_column']
    _update_join_dataset(
        log, configuration, dataset_id, dataset_column, session, collection_id)


def _update_to_url_imageset(session, configuration, ims_url):
    keys = ["dataset_column", "url_template"]
    url_conf = {
        key: configuration.get(key)
        for key in keys if key in configuration
    }
    ims = {
        "name": "Imageset created by CLI",
        "source": {
            "dataset_id": configuration['dataset_id'],
            "transfer": {
                "url": url_conf,
            }
        }
    }
    http.put_json(session, ims_url, ims)


def delete(log, session, args):
    """Delete an image set."""
    log('imageset id: {highlight}{id}{reset}',
        highlight=Fore.GREEN,
        id=args.id,
        reset=Style.RESET_ALL)
    log.warn('delete imageset command coming soon.')


def _resolve_paths(paths):
    """Resolve all paths to a list of files."""
    allowed_ext = tuple(MIMES.keys())

    resolved = []
    for path in paths:
        # this could be rewritten with comprehension and might read more nicely
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file.endswith(allowed_ext):
                        resolved.append(os.path.join(root, file))
        elif os.path.isfile(path) and path.lower().endswith(allowed_ext):
            resolved.append(path)
    return resolved


def _upload_image(path, session, create_url, complete_url, log):
    file_name = os.path.basename(path)
    file_ext = os.path.splitext(path)[-1]
    file_mime = MIMES.get(file_ext, MIMES['.png'])

    with open(path, 'rb') as f:
        info = {
            "image": {
                "blob_id": str(uuid.uuid4()),
                "name": file_name,
                "size": os.path.getsize(path),
                "mimetype": file_mime
            }
        }
        try:
            # First create file object
            create = http.post_json(session, create_url, info)
            # Post file to storage location
            url = create["url"]
            if url.startswith("/"):
                url = 'https://storage.googleapis.com{}'.format(url)
            http.put_file(session, url, f, file_mime)
            # confirm
            http.post_json(session, complete_url, info)
        except Exception as ex:
            log.error("Upload failed: {}".format(ex))
