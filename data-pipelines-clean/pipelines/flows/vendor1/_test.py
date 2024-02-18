def test_sample():
    from .vendor1_reports import Vendor1Flow

    flow = Vendor1Flow("midsomer_murders", ENVIRONMENT="dev", TEST_RUN=True)
    result = flow.run(run_on_schedule=False)
    assert result.is_successful()
