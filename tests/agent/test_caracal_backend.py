from iris.agent.backend.caracal import probe
from tests.helpers import superuser


@superuser
def test_probe(agent_settings, tmp_path):
    excluded_filepath = tmp_path / "excluded.csv"
    excluded_filepath.write_text("8.8.4.4/32")
    probes_filepath = tmp_path / "probes.csv"
    probes_filepath.write_text(
        "8.8.8.8,24000,33434,32,icmp\n8.8.4.4,24000,33434,32,icmp"
    )
    results_filepath = tmp_path / "results.csv"
    prober_statistics = {}
    agent_settings.AGENT_CARACAL_EXCLUDE_PATH = excluded_filepath
    probe(
        agent_settings,
        probes_filepath,
        results_filepath,
        1,
        None,
        100,
        prober_statistics,
    )
    assert prober_statistics["packets_sent"] == 1
    assert prober_statistics["filtered_prefix_excl"] == 1
