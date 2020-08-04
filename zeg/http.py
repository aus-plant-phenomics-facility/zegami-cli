# Copyright 2018 Zegami Ltd

"""Tools for making http requests."""

import requests.auth
import urllib3.util.retry

API_START_FORMAT = "{prefix}/api/v0/project/{project_id}/"

# Max number of api requests to make at once.
CONCURRENCY = 16


class ClientError(Exception):
    """Failure when using http api."""

    def __init__(self, response, try_json=True):
        self.code = response.status_code
        if try_json:
            try:
                body = response.json()
            except ValueError:
                body = response.content
        else:
            body = response.content
        self.body = body

    def __repr__(self):
        return '<{} {} {}>'.format(
            self.__class__.__name__, self.code, self.body)

    def __str__(self):
        return '{} {!r}'.format(self.code, self.body)


class TokenEndpointAuth(requests.auth.AuthBase):
    """Request auth that adds bearer token for specific endpoint only."""

    def __init__(self, endpoint, token):
        """Initialise auth helper."""
        self.endpoint = endpoint
        self.token = token

    def __call__(self, request):
        """Call auth helper."""
        if request.url.startswith(self.endpoint):
            request.headers["Authorization"] = "Bearer {}".format(self.token)
        return request


def get_api_url(url_prefix, project_id):
    """Get the formatted API prefix."""
    return API_START_FORMAT.format(
        prefix=url_prefix,
        project_id=project_id)


def make_session(endpoint, token):
    """
    Create a session object with optional auth handling and a retry backoff.

    See https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    """
    session = requests.Session()

    # Retry post requests as well as the usual methods.
    retry_methods = urllib3.util.retry.Retry.DEFAULT_METHOD_WHITELIST.union(
        ('POST', 'PUT'))
    retry = urllib3.util.retry.Retry(
        total=10,
        backoff_factor=0.5,
        status_forcelist=[(502, 503, 504, 408)],
        method_whitelist=retry_methods
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    if token is not None:
        session.auth = TokenEndpointAuth(endpoint, token)
    return session


def handle_response(response):
    is_204 = response.status_code == 204
    is_200 = response.status_code == 200
    is_201 = response.status_code == 201

    if response.status_code >= 300:
        raise ClientError(response)
    elif (is_204 or is_201 or is_200 and not response.content):
        return None
    try:
        json = response.json()
    except ValueError:
        raise ClientError(response, try_json=False)
    return json


def get(session, url):
    """Get a json response."""
    with session.get(url) as response:
        return handle_response(response)


def post_json(session, url, python_obj):
    """Send a json request and decode json response."""
    with session.post(url, json=python_obj) as response:
        return handle_response(response)


def post_file(session, url, name, filelike, mime):
    """Send a data file."""
    details = (name, filelike, mime)
    with session.post(url, files={'file': details}) as response:
        return handle_response(response)


def delete(session, url):
    """Delete a resource."""
    with session.delete(url) as response:
        return handle_response(response)


def put_file(session, url, filelike, mimetype):
    """Put binary content and decode json respose."""
    headers = {'Content-Type': mimetype}
    headers.update(get_platform_headers(url))
    with session.put(url, data=filelike, headers=headers) as response:
        return handle_response(response)


def put_json(session, url, python_obj):
    headers = get_platform_headers(url)
    """Put json content and decode json response."""
    with session.put(url, json=python_obj, headers=headers) as response:
        return handle_response(response)


def put(session, url, data, content_type):
    """Put data and decode json response."""
    headers = {'Content-Type': content_type}
    headers.update(get_platform_headers(url))
    with session.put(url, data=data, headers=headers) as response:
        return handle_response(response)


def get_platform_headers(url):
    # Uploading blobs to azure requires us to specify the kind of blob
    # Block blobs are typical object storage
    # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction#blobs
    if 'windows.net' in url:
        return {'x-ms-blob-type': 'BlockBlob'}
    return {}
