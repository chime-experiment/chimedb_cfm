"""FileReservationTag and FileReservation tables
"""

import peewee as pw

from chimedb.core.orm import base_model
from chimedb.data_index import ArchiveFile, StorageNode


class FileReservationTag(base_model):
    """The list of available tags for files.

    Attributes
    ----------
    name : str
        The tag name
    creator : str
        The username of the creator of this tag
    creation_date : datetime
        The time when this tag was created
    description : str, optional
        A description of the tag
    """

    name = pw.CharField(max_length=64, unique=True)
    creator = pw.CharField(max_length=64)
    creation_time = pw.DateTimeField()
    description = pw.TextField(null=True)


class FileReservation(base_model):
    """ArchiveFile reservation records.

    Attributes
    ----------
    file : foreign key to ArchiveFile
        The file being reserved
    node : foreign key to StorageNode
        The storage node on which the file is reserved
    tag : foreign key to FileReservationTag
        The tag reserving the file
    """

    file = pw.ForeignKeyField(ArchiveFile, backref="reservations")
    node = pw.ForeignKeyField(StorageNode, backref="reservations")
    tag = pw.ForeignKeyField(FileReservationTag, backref="reservations")

    def reserved_in_node(self, file: ArchiveFile, node: StorageNode):
        """Is `file` reserved in `node`?

        Parameters
        ----------
        file : ArchiveFile
            The file to check the reservation of
        node : StorageNode
            The node to check the reservation of

        Returns
        -------
        tags : list of FileReservationTag
            If the file has reservations in `node`, this a the list
            of `FileReservationTag` values reserving this file.
            If the file has no reservations, this is the empty list.
        """
        return [
            rec.tag
            for rec in FileReservation.select(FileReservation.tag)
            .where(FileReservation.file == file, FileReservation.node == StorageNode)
            .execute()
        ]
