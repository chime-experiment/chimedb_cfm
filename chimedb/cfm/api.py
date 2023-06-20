"""Nemo Public API"""
from __future__ import annotations
from typing import TYPE_CHECKING, Union, Tuple

import enum
import pathlib
import peewee as pw
from tabulate import tabulate
from collections import defaultdict

import chimedb.core as db
from chimedb.data_index import (
    ArchiveFile,
    ArchiveAcq,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    StorageNode,
    StorageGroup,
)

from .tags import FileReservationTag, FileReservation

if TYPE_CHECKING:
    import pathlib

    # A file may be specified via (nicest first):
    #  * an ArchiveFile record
    #  * a 2-tuple with two strings containing an acquisition name and a
    #    file name
    #  * a pathlib.Path
    #  * a string with a path
    FileSpec = Union[str, pathlib.Path, Tuple[str, str], ArchiveFile]


class CopyState(enum.Flag):
    """State bitmask for managed file copies:

    Flags:
        * PRESENT: file is present on the managed node
        * AVAILABLE: file is present on the source node(s)
        * RESERVED: reservation exists for current tag
        * RECALLED: a request was made to recall the file
    """

    PRESENT = enum.auto()
    AVAILABLE = enum.auto()
    RESERVED = enum.auto()
    RECALLED = enum.auto()


class FileManager:
    """Base object for data management.

    In general, on cedar, you should use the pre-configured
    `CedarManager` subclass, which is already set up for
    you, rather than instantiating this class directly.

    Parameters
    ----------
    node
        The destination node.

    Raises
    ------
    ValueError
        `node.io_class` was not "Reserving".

    """

    __slots__ = ["_node", "_sources", "_tag"]

    def __init__(self, node: StorageNode) -> None:
        # if node.io_class != "Reserving":
        #    raise ValueError("non-Reserving node provided.")

        self._node = node
        self._sources = list()
        self._tag = None

        # Connect to the database
        db.connect(read_write=True)

    def add_source(self, src: StorageNode | StorageGroup) -> None:
        """Add `src` as a data source.

        If `src` is a group, all nodes in the group are added.

        Parameters
        ----------
        src
            The source.

        Raises
        ------
        TypeError
            src was neither a StorageNode neither a StorageGroup
        """

        if isinstance(src, StorageGroup):
            for node in StorageNode.select().where(StorageNode.group == src):
                self.add_source(node)
        elif isinstance(src, StorageNode):
            self._sources.append(src)
        else:
            raise TypeError("expected StorageNode or StorageGroup")

    def use_tag(self, tag: FileReservationTag | str) -> None:
        """Use `tag` for future reservations."""

        if not isinstance(tag, FileReservationTag):
            tag = FileReservationTag.get(name=tag)

        self._tag = tag

    def _autotags(self, tags):
        if tags:
            return tags

        if self._tag is None:
            raise ValueError("no tag in use")

        return [self._tag]

    # Reservation querying methods
    # These need no sources to work

    def all_reserved_files(
        self, tags: list(FileReservationTag) | None = None
    ) -> list(ArchiveFile):
        """List all files reserved by tag.

        Parameters:
        -----------
        tags : optional
            List files reserved by at least one of the given `tags`.
            If this is omitted, or None, the currently in-use tag is used.

        Returns:
        --------
        files
            The ArchiveFiles reserved by the current tag on this node.
            May be the empty list.

        Raises
        ------
        ValueError:
            No `tags` were given and no tag is in use.  (See `use_tag`.)
        """

        # This raises ValueError if both tags and self._tag are None/empty.
        tags = self._autotags(tags)

        # query
        files = (
            ArchiveFile.select(ArchiveFile)
            .join(FileReservation)
            .where(FileReservation.node == self._node, FileReservation.tag << tags)
        )

        return list(files)

    def reserved_size(self, tags: list(FileReservationTag) | None = None) -> int:
        """Total size of files reserved by the current tag.

        Parameters:
        -----------
        tags : optional
            List files reserved by at least one of the given `tags`.
            If this is omitted, or None, the currently in-use tag is used.

        Returns
        -------
        size
            The total size in bytes.

        Raises
        ------
        ValueError:
            No `tags` were given and no tag is in use.  (See `use_tag`.)
        """

        # This raises ValueError if both tags and self._tag are None/empty.
        tags = self._autotags(tags)

        # query
        size = (
            ArchiveFile(pw.SUM(ArchiveFile.size_b))
            .join(FileReservation)
            .where(FileReservation.node == self._node, FileReservation.tag << tags)
        ).scalar()

        return size

    def is_reserved(self, file: FileSpec) -> bool:
        """Has the current tag reserved `file`?"""
        return self.reserve(file, check_only=True)

    def _fixup_filespec(self, files: FileSpec | list(FileSpec)) -> list(ArchiveFile):
        """Convert `files` into a list of `ArchiveFile` records.

        A single file may be specified one of three ways:
            * a string containing a path
            * a Pathlib.Path containing a path
            * an ArchiveFile record itself
            * a 2-tuple of strings: (acq-name, file-name)
        The paths may be absolute or relative.  If absolute, the _must_ include
        the storage node root.  If relative, they're assumed to be relative to that path.

        Parameters
        ----------
        files:
            Either a single file specification, as outlined above, or else a list
            of the same (which is allowed to be empty).

        Returns
        -------
        files: list of ArchiveFiles
            this is always a list, even in the case of a single file spec.

        Raises
        ------
        ValueError
            At least one file spec was not relative to node.root (if an
            absolute path), or did not refer to a valid file.
        """
        files_out = list()

        # if we don't have a list, first step is to listify
        if not isinstance(files, list):
            files = [files]

        # Now loop over files and convert.
        for file in files:
            # We essentially do a type cascade here:
            # str -> pathlib -> 2-tuple -> ArchiveFile

            # Convert str to pathlib, if necessary
            if isinstance(file, str):
                file = pathlib.Path(file)

            # Convert pathlib to 2-tuple:
            if isinstance(file, pathlib.Path):
                # Make relative
                if file.is_absolute:
                    # Raises ValueError on failure
                    file = file.relative_to(self._node.root)

                file = file.parts

            # Look-up ArchiveFile from 2-tuple:
            if isinstance(file, tuple):
                if len(file) != 2:
                    raise ValueError(f"Expected two path elements, but got: {file}")

                # Find the file
                try:
                    file = (
                        ArchiveFile.select()
                        .join(ArchiveAcq)
                        .where(
                            ArchiveAcq.name == file[0],
                            ArchiveFile.name == file[1],
                        )
                        .get()
                    )
                except pw.NotFoundError:
                    raise ValueError(f"No such file: {file[0]}/{file[1]}")

            # Finally we have an ArchiveFile, append it to the output list
            files_out.append(file)

        # Return the converted list
        return files_out

    def _filecopy_state(self, file: ArchiveFile) -> Tuple[CopyState, StorageNode]:
        """What is the state of `file` for the current tag?

        Also returns the source node containing `file`, if any."""

        state = CopyState(0)
        source = None

        # Is it on the destination?
        try:
            ArchiveFileCopy.get(
                ArchiveFileCopy.file == file,
                ArchiveFileCopy.node == self._node,
                ArchiveFileCopy.has_file == "Y",
            )
            state |= CopyState.PRESENT
        except pw.DoesNotExist:
            pass

        # Is it on at least one of the sources?
        # If so, remember which one
        try:
            copy = ArchiveFileCopy.get(
                ArchiveFileCopy.file == file,
                ArchiveFileCopy.node << self._sources,
                ArchiveFileCopy.has_file == "Y",
            )
            state |= CopyState.AVAILABLE
            source = copy.node
        except pw.DoesNotExist:
            pass

        # Is it reserved?
        try:
            FileReservation.get(
                FileReservation.file == file,
                FileReservation.tag == self._tag,
                FileReservation.node == self._node,
            )
            state |= CopyState.RESERVED
        except pw.DoesNotExist:
            pass

        return state, source

    def reserve(
        self,
        files: FileSpec | list(FileSpec),
        check_only: bool = False,
        verbose: bool = False,
    ) -> bool:
        """Reserve `file` using the current tag.

        Parameters
        ----------
        files:
            The file or files to reserve.  If specified as a path,
            should be relative to the node root.
        check_only: optional
            If set to `True`, new copy requests will not be made if `files`
            aren't already available on the node.  In this case, the only
            result of calling this method is its return value.
        verbose: optional
            if set to `True`, a summary of the current state of the `files`
            is written to the terminal.

        Returns
        -------
        present : bool
            True if all reserved files are already on the storage node.
            If this is False, and `check_only` wasn't set to True, then
            this call will have created requests to have the missing files
            transferred from the source nodes onto the storage node.
        """
        files = self._fixup_filespec(files)

        result = True

        states = dict()

        for file in files:
            # Get current state:
            state, source = self._filecopy_state(file)

            # Update result
            if result and CopyState.PRESENT not in state:
                result = False

            # Skip all this if we're only checking
            if not check_only:
                # Reserve the file
                if CopyState.RESERVED not in state:
                    FileReservation.create(file=file, node=self._node, tag=self._tag)

                # Recall the file
                if (
                    state & (CopyState.PRESENT | CopyState.AVAILABLE)
                    == CopyState.AVAILABLE
                ):
                    _, created = ArchiveFileCopyRequest.get_or_create(
                        file=file,
                        group_to=self._node.group,
                        node_from=source,
                        nice=0,
                        cancelled=False,
                        completed=False,
                        n_requests=0,
                        timestamp=0,
                    )
                    if created:
                        state |= CopyState.RECALLED

            # Remember
            states[file] = state

        # Verbose report
        if verbose:
            # Descriptions of states
            def _state_name(state, check_only):
                # This is the reservation state at the _start_
                if CopyState.RESERVED in state:
                    name = "Previously reserved"
                elif check_only:
                    name = "Unreserved"
                else:
                    name = "Newly reserved"

                if CopyState.PRESENT in state:
                    return name + ", present"

                if CopyState.RECALLED in state:
                    return name + ", recalling"

                if CopyState.AVAILABLE in state:
                    return name + ", available"

                return name + ", missing"

            # Tot up things
            file_totals = defaultdict(int)
            byte_totals = defaultdict(int)

            for file, state in states.items():
                name = _state_name(state, check_only)
                file_totals[name] += 1
                byte_totals[name] += file.size_b

            # tabulate
            table_data = [
                [name, file_totals[name], byte_totals[name] / 1e9]
                for name in file_totals
            ]

            # Now print
            print(
                tabulate(
                    table_data,
                    headers=["State", "Files", "GB"],
                    intfmt=",",
                    floatfmt=",.3f",
                )
            )

        return result


class CedarManager(FileManager):
    """A FileManager pre-configured for CHIME use on cedar.

    Parameters
    ----------
    tag
        The reservation tag to use.  Can be changed later
        via `use_tag`.
    """

    def __init__(self, tag: FileReservationTag | str) -> None:
        super().__init__(StorageNode.get(name="cedar_online"))
        self.add_source(StorageGroup.get(name="cedar_nearline"))
        self.use_tag(tag)
