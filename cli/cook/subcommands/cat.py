from functools import partial

from cook.querying import query_unique_and_run
from cook.util import strip_all


def cat_for_instance(instance, sandbox_dir, path):
    """Outputs the contents of the Mesos sandbox path for the given instance."""


def cat(clusters, args):
    """Outputs the contents of the corresponding Mesos sandbox path by job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    paths = strip_all(args.get('path'))

    if len(uuids) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    if len(paths) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single path.')

    command_fn = partial(cat_for_instance, path=paths[0])
    query_unique_and_run(clusters, uuids[0], command_fn)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('cat', help='output file contents by job or instance uuid')
    parser.add_argument('uuid', nargs=1)
    parser.add_argument('path', nargs=1)
    return cat
