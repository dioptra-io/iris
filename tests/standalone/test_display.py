import uuid
from contextlib import redirect_stdout
from datetime import datetime

from diamond_miner.queries import results_table

from iris.standalone.display import display_results


def test_display_results():
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    start_time = datetime.now()
    end_time = datetime.now()

    results = {
        "measurement_uuid": measurement_uuid,
        "agent_uuid": agent_uuid,
        "database_name": "iris",
        "table_name": results_table(f"{measurement_uuid}__{agent_uuid}"),
        "n_rounds": 1,
        "min_ttl": 5,
        "start_time": start_time,
        "end_time": end_time,
        "n_nodes": 10,
        "n_links": 20,
    }

    class FakeStdout(object):
        count: int = 0

        def write(self, *args, **kwargs):
            self.count += 1
            pass

        def flush(*args, **kwargs):
            pass

    stdout = FakeStdout()
    with redirect_stdout(stdout):
        display_results(results)

    assert stdout.count == 2
