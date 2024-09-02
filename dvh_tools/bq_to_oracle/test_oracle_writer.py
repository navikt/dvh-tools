from .oracle_writer import OracleWriter
import pytest
import datetime


@pytest.fixture()
def batch():
    """Fixture that provides a sample batch of data for testing.

    This fixture creates a list of dictionaries, each representing a batch of data. 
    The dictionaries include various fields such as primary keys, metadata, and lists.
    
    Returns:
        batch (list): A list of dictionaries, each representing a sample batch of data.

    Examples:
        >>> batch = batch()
        >>> print(batch)
        [{'pk': 0, 'metadata': '2024-08-30 12:00:00', 'liste': '["ele1", "ele2"]', 'liste2': '[{"key": "val0"}]'},
         {'pk': 1, 'metadata': '2024-08-30 12:00:00', 'liste': '["ele1", "ele2"]', 'liste2': '[{"key": "val1"}]'}]
    """
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


def test_convert_lists_and_dicts_in_batch_to_json(batch) -> None:
    """Tests the conversion of lists and dictionaries to JSON strings in a batch of data.

    This test verifies that the `convert_lists_and_dicts_in_batch_to_json` method of 
    `OracleWriter` converts lists and dictionaries in the provided batch to JSON strings.

    Args:
        batch (list): A sample batch of data as created by the `batch` fixture.

    Examples:
        >>> batch = batch()
        >>> test_convert_lists_and_dicts_in_batch_to_json(batch)
        # Checks if 'metadata', 'liste', and 'liste2' fields in 'batch' are converted to strings.
    """
    OracleWriter.convert_lists_and_dicts_in_batch_to_json(batch)
    assert isinstance(batch[0]["metadata"], str)
    assert isinstance(batch[0]["liste"], str)
    assert isinstance(batch[0]["liste2"], str)
