# Copyright 2018-2020 Zegami Ltd

"""Collection commands."""
import os
import sys

import yaml
import jsonschema


def parse_args(args, log):
    # check for config
    if 'config' not in args:
        log.error('Configuration file path missing')
        sys.exit(1)
    configuration = parse_config(args.config, log)
    for attr in ['id', 'project', 'url']:
        if attr in args:
            configuration[attr] = getattr(args, attr)
    return configuration


def parse_config(path, log):
    """Parse yaml collection configuration."""
    configuration = load_config(path)
    validate_config(configuration, log)
    return configuration


def load_config(path):
    """Load yaml collection configuration."""
    with open(path, 'r') as stream:
        return yaml.load(stream, Loader=yaml.SafeLoader)


def validate_config(configuration, log):
    schema_path = os.path.join(
        os.path.dirname(__file__),
        'schemata',
        'spec.yaml'
    )
    with open(schema_path, 'r') as stream:
        schema = yaml.load(stream, Loader=yaml.SafeLoader)

    try:
        jsonschema.validate(configuration, schema)
    except jsonschema.exceptions.ValidationError as ex:
        log.error(
            'Configuration validation error. {err}',
            err=ex.message
        )
        raise ex
