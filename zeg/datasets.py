# Copyright 2018 Zegami Ltd.

"""Collection commands."""

import os
import sys
from datetime import datetime
from tempfile import mkstemp
import uuid

from colorama import Fore, Style

from . import (
    config,
    http,
    sql,
)


MIMES = {
    ".tab": "text/tab-separated-values",
    ".tsv": "text/tab-separated-values",
    ".csv": "text/csv",
    ".xlsx": ("application/vnd.openxmlformats-officedocument"
              ".spreadsheetml.sheet"),
}


def get(log, session, args):
    """Get a data set."""
    url = "{}datasets/{}".format(
        http.get_api_url(args.url, args.project),
        args.id)
    log.debug('GET: {}'.format(url))
    response_json = http.get(session, url)
    log.print_json(response_json, "dataset", "get")


def update(log, session, args):
    configuration = config.parse_args(args, log)
    update_from_dict(log, session, configuration)


def update_from_dict(log, session, configuration):
    """Update a data set."""
    url_url = "{}imagesets/{}/image_url".format(
        http.get_api_url(configuration['url'], configuration['project']),
        configuration['id'])
    replace_url = "{}datasets/{}/".format(
        http.get_api_url(configuration['url'], configuration['project']),
        configuration['id'])

    log.debug('POST: {}'.format(url_url))
    # get update config
    if 'file_config' in configuration:
        (
            file_path,
            extension,
            file_mime
        ) = _file_type_update(log, configuration['file_config'])
    elif 'sql_config' in configuration:
        (
            file_path,
            extension,
            file_mime
        ) = _sql_type_update(log, configuration['sql_config'])

    log.debug("File path: {}".format(file_path))
    log.debug("File extension: {}".format(extension))
    log.debug("File mime: {}".format(file_mime))
    file_name = os.path.basename(file_path)

    with open(file_path, 'rb') as f:
        blob_id = str(uuid.uuid4())
        info = {
            "image": {
                "blob_id": blob_id,
                "name": file_name,
                "size": os.path.getsize(file_path),
                "mimetype": file_mime
            }
        }
        create = http.post_json(session, url_url, info)
        # Post file to storage location
        url = create["url"]
        if url.startswith("/"):
            url = 'https://storage.googleapis.com{}'.format(url)
        log.debug('PUT (file content): {}'.format(url))
        data = f.read()
        http.put(session, url, data, file_mime)

        # confirm
        log.debug('GET (dataset): {}'.format(replace_url))
        current = http.get(session, replace_url)["dataset"]
        current["source"].pop("schema", None)
        current["source"]["upload"] = {
            "name": file_name,
        }
        current["source"]["blob_id"] = blob_id
        log.debug('PUT (dataset): {}'.format(replace_url))
        http.put_json(session, replace_url, current)
        log.print_json(current, "dataset", "update")


def delete(log, args):
    """Delete an data set."""
    log('dataset id: {highlight}{id}{reset}',
        highlight=Fore.GREEN,
        id=args.id,
        reset=Style.RESET_ALL)
    log.warn('delete dataset command coming soon.')


def _file_type_update(log, file_config):
    """Load file and update data set."""
    if 'path' in file_config:
        file_path = file_config['path']
    elif 'directory' in file_config:
        file_path = _get_most_recent_file(file_config['directory'])

    if file_path is None:
        log.error('Data file not found.')
        sys.exit(1)

    file_ext = os.path.splitext(file_path)[-1]
    file_mime = MIMES.get(file_ext, MIMES['.csv'])
    return (file_path, file_ext, file_mime)


def _sql_type_update(log, sql_config):
    """Query database and convert to csv file."""
    if not sql.have_driver:
        log.error('No sql driver found, is sqlalchemy installed?')
        sys.exit(1)

    statement = sql.create_statement(sql_config['query'])
    engine = sql.create_engine(sql_config['connection'], log.verbose)
    mime_type = '.csv'
    with engine.connect() as connection:
        result = connection.execute(statement)
        # write to a comma delimited file
        fd, name = mkstemp(suffix=mime_type, prefix='zeg-dataset')
        with open(fd, 'w') as output:
            # write headers
            output.write(str(result.keys())[1:-1] + '\n')
            for row in result:
                row_as_string = [_handle_sql_types(value) for value in row]
                output.write(str(row_as_string)[1:-1] + '\n')
    return (name, mime_type, MIMES[mime_type])


def _handle_sql_types(value):
    """Convert types into Zegami friendly string format."""
    if type(value) is datetime:
        return value.isoformat()
    return str(value)


def _newest_ctime(entry):
    return -entry.stat().st_ctime, entry.name


def _get_most_recent_file(path):
    """Get the most recent file in a directory."""
    allowed_ext = tuple(MIMES.keys())
    files_iter = (
        entry for entry in os.scandir(path)
        if entry.is_file() and entry.name.lower().endswith(allowed_ext)
    )
    for entry in sorted(files_iter, key=_newest_ctime):
        return entry.path
    return None
