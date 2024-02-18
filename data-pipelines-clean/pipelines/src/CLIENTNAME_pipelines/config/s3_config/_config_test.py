def test_config():
    from CLIENTNAME_pipelines.config import Config

    conf = Config(ENVIRONMENT="dev")
    assert conf
