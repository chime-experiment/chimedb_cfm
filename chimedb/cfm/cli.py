"""CHIME file manager CLI"""
from __future__ import annotations
from typing import TextIO

import os
import time
import click
import shlex
import tempfile
from subprocess import Popen

import chimedb.core as db

from .api import CedarManager


@click.group
def cli():
    """This is the Cedar File Manager.  It is not documented."""
    pass


@cli.group
def tag():
    """Manage reservation tags"""


def get_editor(context: click.Context) -> str:
    """Figure out the user's preferred editor.

    Essentially the same as how git does it.
    """

    # This is not recommended, but if someone really
    # wants a CFM-specific editor, we'll support it.
    try:
        return os.environ["CFM_EDITOR"]
    except KeyError:
        pass

    # Are we using a dumb terminal?
    term = os.environ.get("TERM", "dumb")
    terminal_is_dumb = "dumb" in term

    # the VISUAL editor only works on non-dumb terminals
    if not terminal_is_dumb:
        try:
            return os.environ["VISUAL"]
        except KeyError:
            pass

    # the EDITOR works on all terminals
    try:
        return os.environ["EDITOR"]
    except KeyError:
        pass

    # Git only defines a fallback for non-dumb terminals
    if not terminal_is_dumb:
        # NB: if "vi" is vim, you really should be running
        # with the -f flag (which standard vi does not support).

        # Be obnoxious
        click.echo('No editor found.  Falling back to "vi".', err=True)
        time.sleep(1)

        return "vi"

    # No editor found
    context.fail("No editor found.  Specify one with environmental variable CFM_EDITOR or EDITOR.")



@tag.command
@click.pass_context
@click.option(
    "--description",
    "-d",
    metavar="DESC",
    help="The description of the tag.  If not set, an editor will be spawned to give you the opportunity to enter the description.",
)
@click.option(
    "--user",
    "-u",
    metavar="NAME",
    type=str,
    help="The user creating the tag.  If not set, your username is used.",
)
@click.argument("tag", type=str)
def new(
    context: click.Context,
    description: str | None,
    user: str | None,
    tag: str,
):
    """Create a new tag called TAG."""

    # If no user, use username
    if user is None:
        user = os.getlogin()

    if description is None:
        editor = get_editor(context)

        # Create file
        with tempfile.NamedTemporaryFile(prefix="cfm-", delete=False) as f:
            f.write(
                b'# Enter the description of the tag "' + tag.encode() + b'"\n'
                b"# Lines beginning with the # will be ignored."
            )
            f.close()

            # Now open in the external editor for the user
            args = shlex.split(editor)
            args.append(f.name)
            proc = Popen(args)
            proc.wait()


@cli.command
@click.option(
    "--check",
    "-c",
    is_flag=True,
    help="Check only: don't make any changes to the database.",
)
@click.option(
    "--tag", "-t", metavar="TAG", type=str, help="Use reservation TAG to reserve files."
)
@click.option(
    "--read-from",
    "-f",
    type=click.File(mode="r"),
    metavar="TEXTFILE",
    help='read FILEs from TEXTFILE.  If TEXTFILE is "-", read from standard input.',
)
@click.argument("envtag", envvar="CHIMEFM_TAG", type=str)
@click.argument("file_", metavar="FILE", nargs=-1, type=str)
def reserve(
    check: bool,
    tag: str | None,
    read_from: TextIO | None,
    envtag: str | None,
    file_: tuple | None,
):
    """Reserve FILE(s)."""

    # If no tag is specified, look for an environmental tag
    if tag is None:
        if envtag is None:
            click.UsageError("No tag specified.")
        else:
            tag = envtag

    # Create a list of files
    files = list(file_)

    if read_from:
        files.append(read_from.readlines())

    # Create the manager
    cm = CedarManager(tag)

    # Reserve all the things
    cm.reserve(files, check_only=check, verbose=True)
