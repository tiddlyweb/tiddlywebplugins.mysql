
from tiddlyweb.config import config

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.bag import Bag

from tiddlywebplugins.utils import get_store

from tiddlywebplugins.mysql import index_query

def setup_module(module):
    module.store = get_store(config)
    module.environ = {'tiddlyweb.config': config,
            'tiddlyweb.store': module.store}


def test_simple_store():
    bag = Bag('bag1')
    store.put(bag)
    tiddler = Tiddler('tiddler1', 'bag1')
    tiddler.text = 'oh hello i chrisdent have nothing to say here you know'
    tiddler.tags = ['apple', 'orange', 'pear']
    tiddler.fields['house'] = 'cottage'
    store.put(tiddler)

    retrieved = Tiddler('tiddler1', 'bag1')
    retrieved = store.get(retrieved)

    assert retrieved.text == tiddler.text

def test_simple_search():
    tiddlers = list(store.search('"chrisdent"'))

    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler1'
    assert tiddlers[0].bag == 'bag1'

    tiddlers = list(store.search('hello'))

    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler1'
    assert tiddlers[0].bag == 'bag1'

def test_index_query_id():
    kwords = {'id': 'bag1:tiddler1'}
    tiddlers = list(index_query(environ, **kwords))

    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler1'
    assert tiddlers[0].bag == 'bag1'

def test_index_query_filter():
    kwords = {'tag': 'orange'}
    tiddlers = list(index_query(environ, **kwords))

    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler1'
    assert tiddlers[0].bag == 'bag1'

def test_index_query_filter_fields():
    kwords = {'house': 'cottage'}
    tiddlers = list(index_query(environ, **kwords))

    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler1'
    assert tiddlers[0].bag == 'bag1'
    assert tiddlers[0].fields['house'] == 'cottage'

    kwords = {'house': 'mansion'}
    tiddlers = list(index_query(environ, **kwords))

    assert len(tiddlers) == 0

def test_index_query_filter_fields():
    kwords = {'bag': 'bag1', 'house': 'cottage'}
    tiddlers = list(index_query(environ, **kwords))

    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler1'
    assert tiddlers[0].bag == 'bag1'
    assert tiddlers[0].fields['house'] == 'cottage'
