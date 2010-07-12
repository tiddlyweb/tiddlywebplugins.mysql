


def test_compile():
    try:
        import tiddlywebplugins.mysql
        assert True
    except ImportError, exc:
        assert False, exc
