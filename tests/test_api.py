"""Tests for chimefm.api."""

import chimefm.api as cfm
import chimedb.data_index as di

from chimefm.tags import FileReservation, FileReservationTag


def test_add(test_data):
    """Tests adding a new reservation."""

    tag = FileReservationTag.get(name="tag1")
    cedar = cfm.CedarManager(tag=tag)

    file2 = di.ArchiveFile.get(name="file2")

    result = cedar.reserve(di.ArchiveFile.get(name="file2"))
    assert result is False

    # Check for reservation
    res = FileReservation.get(file=file2, tag=tag)
    assert res.node.name == "cedar_online"
