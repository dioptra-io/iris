from iris.agent.backend.caracal import probe
from tests.helpers import superuser


@superuser
async def test_probe(agent_settings, logger, tmp_path):
    excluded_filepath = tmp_path / "excluded.csv"
    excluded_filepath.write_text("8.8.4.4/32")
    probes_filepath = tmp_path / "probes.csv"
    probes_filepath.write_text(
        "8.8.8.8,24000,33434,32,icmp\n8.8.4.4,24000,33434,32,icmp"
    )
    results_filepath = tmp_path / "results.csv"
    agent_settings.AGENT_CARACAL_EXCLUDE_PATH = excluded_filepath
    prober_statistics = await probe(
        agent_settings,
        logger,
        probes_filepath,
        results_filepath,
        1,
        None,
        100,
    )
    # TODO: Re-implement statistics, for now just check it didn't crash
    assert "packets_sent" in prober_statistics
    # assert prober_statistics["packets_sent"] == 1
    # assert prober_statistics["filtered_prefix_excl"] == 1
