from .oracle_writer import OracleWriter

import pytest
import datetime


@pytest.fixture()
def batch():
    batch = []
    for i in range(2):
        batch.append(
            {
                "pk": i,
                "metadata": {"oppretted_tid": str(datetime.datetime.now())},
                "liste": ["ele1", "ele2"],
                "liste2": [{"key": "val{}".format(i)}],
            }
        )
        return batch


def test_convert_lists_and_dicts_in_batch_to_json(batch):
    OracleWriter.convert_lists_and_dicts_in_batch_to_json(batch)
    assert isinstance(batch[0]["metadata"], str)
    assert isinstance(batch[0]["liste"], str)
    assert isinstance(batch[0]["liste2"], str)
