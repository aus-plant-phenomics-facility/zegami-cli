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


def _get_chunk_upload_futures(
    executor, paths, session, create_url,
    complete_url, log, workload_size, offset, mime
):
    """Return executable tasks with image uploads in batches.

    Instead of performing image uploads and updating the imageset
    one at a time, we reduce load on the API server by uploading
    many images and updating the api server in one go, consequently
    making upload faster.
    """
    total_work = len(paths)
    workloads = []
    temp = []

    i = 0
    while i < total_work:
        path = paths[i]
        temp.append(path)
        i += 1
        if len(temp) == workload_size or i == total_work:
            workload_info = {
                "start": i - len(temp) + offset,
                "count": len(temp),
            }
            workloads.append(executor.submit(
                _upload_image_chunked,
                temp,
                session,
                create_url,
                complete_url,
                log,
                workload_info,
                mime
            ))
            temp = []

    return workloads


def _upload_image_chunked(paths, session, create_url, complete_url, log, workload_info, mime):  # noqa: E501
    results = []

    # get all signed urls at once
    try:
        id_set = {"ids": [str(uuid.uuid4()) for path in paths]}
        signed_urls = http.post_json(session, create_url, id_set)
    except Exception as ex:
        log.error("Could not get signed urls for image uploads: {}".format(ex))
        return

    index = 0
    for fpath in paths:
        try:
            file_name = os.path.basename(fpath)
            file_ext = os.path.splitext(fpath)[-1]
            if mime is not None:
                file_mime = mime
            else:
                file_mime = MIMES.get(file_ext, MIMES['.jpg'])
        except Exception as ex:
            log.error("issue with file info: {}".format(ex))

        with open(fpath, 'rb') as f:
            blob_id = id_set["ids"][index]
            info = {
                "image": {
                    "blob_id": blob_id,
                    "name": file_name,
                    "size": os.path.getsize(fpath),
                    "mimetype": file_mime
                }
            }
            index = index + 1
            try:
                # Post file to storage location
                url = signed_urls[blob_id]
                # google based signed urls come as a relative path
                if url.startswith("/"):
                    url = 'https://storage.googleapis.com{}'.format(url)
                http.put_file(session, url, f, file_mime)
                # pop the info into a temp array, upload only once later
                results.append(info["image"])
            except Exception as ex:
                log.error("File upload failed: {}".format(ex))

    # send the chunk of images as a bulk operation rather than per image
    try:
        url = complete_url + "?start={}".format(workload_info["start"])
        log.debug("POSTING TO: {}".format(url))
        http.post_json(session, url, {'images': results})
    except Exception as ex:
        log.error("Failed to complete workload: {}".format(ex))


def _update_file_imageset(log, session, configuration):
    bulk_create_url = "{}signed_blob_url".format(
        http.get_api_url(configuration["url"], configuration["project"]))
    bulk_create_url = bulk_create_url.replace('v0', 'v1')
    complete_url = "{}imagesets/{}/images_bulk".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        configuration["id"])
    extend_url = "{}imagesets/{}/extend".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        configuration["id"])
    log.debug('POST: {}'.format(extend_url))
    # log.debug('POST: {}'.format(create_url))
    log.debug('POST: {}'.format(bulk_create_url))
    log.debug('POST: {}'.format(complete_url))

    # get image paths
    file_config = configuration['file_config']
    # check colleciton id, dataset and join column name

    recursive = False
    mime_type = None
    if 'recursive' in file_config:
        recursive = file_config["recursive"]

    if 'mime_type' in file_config:
        mime_type = file_config["mime_type"]

    # first extend the imageset by the number of items we have to upload
    paths = _resolve_paths(
        file_config['paths'], recursive, mime_type is not None
    )

    extend_response = http.post_json(
        session, extend_url, {'delta': len(paths)}
    )
    add_offset = extend_response['new_size'] - len(paths)

    workload_size = optimal_workload_size(len(paths))

    # When chunking work, futures could contain as much as 100 images at once.
    # If the number of images does not divide cleanly into 10 or 100 (optimal)
    # The total may be larger than reality and the image/s speed less accurate.
    if workload_size != 1:
        log.warn("The progress bar may have reduced accuracy when uploading larger imagesets.")  # noqa: E501

    with concurrent.futures.ThreadPoolExecutor(http.CONCURRENCY) as executor:
        # get bundles that will upload images rather than a future per image
        futures = _get_chunk_upload_futures(
            executor,
            paths,
            session,
            bulk_create_url,
            complete_url,
            log,
            workload_size,
            add_offset,
            mime_type
        )
        kwargs = {
            'total': len(futures),
            'unit': 'image',
            'unit_scale': workload_size,
            'leave': True
        }
        for f in tqdm(concurrent.futures.as_completed(futures), **kwargs):
            pass


def optimal_workload_size(count):
    # Just some sensible values aiming to speed up uploading large imagesets
    if count > 2500:
        return 100

    if count < 100:
        return 1

    return 10


def _update_join_dataset(
        log, configuration, dataset_id, dataset_column, session,
        collection_id):
    dataset_create_url = "{}datasets/".format(
        http.get_api_url(configuration["url"], configuration["project"])
    )

    collection_url = "{}collections/{}".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        collection_id,
    )
    log.debug('GET (collection): {}'.format(collection_url))
    # first need to get the collection object
    collection_response = http.get(session, collection_url)
    collection = collection_response['collection']

    # update the join dataset
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
    imageset_dataset_join_url = "{}datasets/{}".format(
        http.get_api_url(configuration["url"], configuration["project"]),
        collection["imageset_dataset_join_id"]
    )
    log.debug('PUT: {}'.format(imageset_dataset_join_url))
    http.put_json(session, imageset_dataset_join_url, join_data)

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
    for_create["source"]["dataset_id"] = collection["imageset_dataset_join_id"]

    # Add new dz json dataset, which will become the used one after processing
    log.debug('POST: {}'.format(dataset_create_url))
    dz_json_response = http.post_json(session, dataset_create_url, for_create)

    # TODO wait until processing finished before switching to new dz_json
    # Point collection at new dz json
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
                "The AZURE_STORAGE_CONNECTION_STRING environment variable"
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


def _resolve_paths(paths, should_recursive, ignore_mime):
    """Resolve all paths to a list of files."""
    allowed_ext = tuple(MIMES.keys())

    resolved = []
    for path in paths:
        whitelisted = (path.lower().endswith(allowed_ext) or ignore_mime)
        if os.path.isdir(path):
            if should_recursive:
                resolved.extend(
                    _scan_directory_tree(path, allowed_ext, ignore_mime)
                )
            else:
                resolved.extend(
                    entry.path for entry in os.scandir(path)
                    if entry.is_file() and (
                        entry.name.lower().endswith(allowed_ext) or ignore_mime
                    )
                )
        elif os.path.isfile(path) and whitelisted:
            resolved.append(path)
    return resolved


def _scan_directory_tree(path, allowed_ext, ignore_mime):
    files = []
    for entry in os.scandir(path):
        whitelisted = (entry.name.lower().endswith(allowed_ext) or ignore_mime)
        if entry.is_file() and whitelisted:
            files.append(entry.path)
        if entry.is_dir():
            files.extend(_scan_directory_tree(entry.path, allowed_ext, ignore_mime))
    return files


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
