


def test_compile():
    try:
        import tiddlywebplugins.mysql3
        assert True
    except ImportError, exc:
        assert False, exc
