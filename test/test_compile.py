


def test_compile():
    try:
        import tiddlywebplugins.mysql2
        assert True
    except ImportError, exc:
        assert False, exc
