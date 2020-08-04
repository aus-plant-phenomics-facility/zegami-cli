#!/usr/bin/env python3
#
# Copyright 2018 Zegami Ltd

"""A command line interface for managing Zegami."""
import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from textwrap import dedent

import jsonschema
import pkg_resources

from . import (
    auth,
    collections,
    datasets,
    http,
    imagesets,
    log,
)


def main():
    """Zegami command line interface."""
    version = pkg_resources.require('zegami-cli')[0].version
    description = dedent(r'''
         ____                      _
        /_  / ___ ___ ____ ___ _  (_)
         / /_/ -_) _ `/ _ `/  ' \/ /
        /___/\__/\_, /\_,_/_/_/_/_/
                /___/  v{}

        Visual data exploration.

    A command line interface for managing Zegami.
    '''.format(version))

    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description=description,
    )

    # top level arguments
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s {}'.format(version),
    )

    option_mapper = {
        'delete': {
            'help': 'Delete a resource',
            'resources': {
                'collections': collections.delete,
                'dataset': datasets.delete,
                'imageset': imagesets.delete,
            }
        },
        'create': {
            'help': 'Create a resource',
            'resources': {
                'collections': collections.create,
            }
        },
        'get': {
            'help': 'Get a resource',
            'resources': {
                'collections': collections.get,
                'dataset': datasets.get,
                'imageset': imagesets.get,
            }
        },
        'publish': {
            'help': 'Publish a resource',
            'resources': {
                'collection': collections.publish,
            }
        },
        'update': {
            'help': 'Update a resource',
            'resources': {
                'collections': collections.update,
                'dataset': datasets.update,
                'imageset': imagesets.update,
            }
        },
    }

    # option mapper parser
    subparsers = parser.add_subparsers()
    for action in option_mapper:
        action_parser = subparsers.add_parser(
            action,
            help=option_mapper[action]['help'],
        )
        # set the action type so we can work out what was chosen
        action_parser.set_defaults(action=action)
        action_parser.add_argument(
            'resource',
            choices=option_mapper[action]['resources'].keys(),
            help='The name of the resource type.'
        )
        if action != "create":
            action_parser.add_argument(
                'id',
                default=None,
                nargs="?",
                help='Resource identifier.',
            )
        action_parser.add_argument(
            '-c',
            '--config',
            help='Path to command configuration yaml.',
        )
        action_parser.add_argument(
            '-p',
            '--project',
            help='The id of the project.',
        )
        _add_standard_args(action_parser)

    # login parser
    login_parser = subparsers.add_parser(
        'login',
        help='Authenticate against the API and store a long lived token',
    )
    login_parser.set_defaults(action='login')
    _add_standard_args(login_parser)

    try:
        args = parser.parse_args()
    except jsonschema.exceptions.ValidationError:
        sys.exit(1)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    logger = log.Logger(args.verbose)
    token = auth.get_token(args)
    session = http.make_session(args.url, token)

    if args.action == 'login':
        auth.login(
            logger,
            session,
            args,
        )
        return

    try:
        option_mapper[args.action]['resources'][args.resource](
            logger,
            session,
            args,
        )
    except Exception as e:
        # unhandled exceptions
        if args.verbose:
            raise e
        logger.error('Unhandled exception: {}'.format(e))
        sys.exit(1)


def _add_standard_args(parser):
    parser.add_argument(
        '-t',
        '--token',
        default=None,
        help='Authentication token.',
    )
    parser.add_argument(
        '-u',
        '--url',
        default='https://zegami.com',
        help='Zegami server address.',
    )
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Enable verbose logging.',
    )


if __name__ == '__main__':
    sys.exit(main())
