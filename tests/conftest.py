"""Common fixtures."""

import pytest
import chimedb.core as db
from chimefm.tags import FileReservation, FileReservationTag
from chimedb.data_index import (
    AcqType,
    ArchiveAcq,
    ArchiveFile,
    ArchiveFileCopy,
    ArchiveFileCopyRequest,
    FileType,
    StorageGroup,
    StorageNode,
)


@pytest.fixture
def proxy():
    """Open a connection to the database.

    Returns the database proxy.
    """
    db.test_enable()
    db.connect(read_write=True)
    yield db.proxy

    db.close()


@pytest.fixture
def tables(proxy):
    """Ensure all the tables are created."""

    proxy.create_tables(
        [
            AcqType,
            ArchiveAcq,
            ArchiveFile,
            ArchiveFileCopy,
            ArchiveFileCopyRequest,
            FileReservation,
            FileReservationTag,
            FileType,
            StorageGroup,
            StorageNode,
        ]
    )


@pytest.fixture
def test_data(tables):
    """Create some test data"""

    # The only acquisition
    acq = ArchiveAcq.create(name="acqpath", type=AcqType.create(name="acqtype"))

    # The file type
    ftype = FileType.create(name="filetype")

    # Make the test (managed) node
    test_node = StorageNode.create(
        name="cedar_online",
        group=StorageGroup.create(name="test_group"),
        root="/test",
        active=True,
        storage_type="F",
        min_avail_gb=1,
    )

    # All the source nodes are in the same group
    group = StorageGroup.create(name="cedar_nearline")

    # Make a few source nodes
    sources = [
        StorageNode.create(
            name=name,
            group=group,
            root="/" + name,
            active=True,
            storage_type="A",
            min_avail_gb=1,
        )
        for name in ["src1", "src2", "src3"]
    ]

    def _create_file(name, size, nodes):
        """Create a file called `name` with
        size `size` and add it to the `nodes` listed.

        File type is `ftype`

        Returns the created ArchiveFile
        """
        nonlocal acq, ftype

        file_ = ArchiveFile.create(acq=acq, name=name, size_b=size, type=ftype)

        for node in nodes:
            ArchiveFileCopy.create(
                file=file_, node=node, has_file="Y", wants_file="Y", size_b=size
            )

        return file_

    # Already on test node
    file0 = _create_file("file0", 123456789, [test_node])

    # Only on test node
    _create_file("file1", 123456789, [test_node, sources[0]])

    # Not on test node
    _create_file("file2", 123456789, [sources[0]])

    # On multiple sources
    _create_file("file3", 123456789, sources)

    # Missing
    _create_file("file4", 123456789, [])

    # Create some tags
    tag1 = FileReservationTag.create(name="tag1")
    FileReservationTag.create(name="tag2")

    # Pre-emptively reserve a file
    FileReservation(file=file0, node=test_node, tag=tag1)
