#!/usr/bin/env python3

import sys

import click

from mapping.msid_mapping import create_mapping
from mapping.recording_pairs import create_pairs
from mapping.test.test_mapping import test_mapping
from mapping.test.test_pairs import test_pairs


@click.command()
@click.argument("action", nargs=1)
def mapping(action):
    if action == 'create-mapping':
        create_mapping()
    elif action == 'create-pairs':
        create_pairs() 
    elif action == 'test':
        test_mapping() 
    else:
        print("unknown action: %s" % action)


def usage(command):
    with click.Context(command) as ctx:
        click.echo(command.get_help(ctx))


if __name__ == "__main__":
    mapping()
    sys.exit(0)
