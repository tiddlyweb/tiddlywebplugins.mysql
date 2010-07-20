
from tiddlyweb.config import config

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.bag import Bag

from tiddlywebplugins.utils import get_store

from tiddlywebplugins.mysql import index_query
from tiddlywebplugins.sqlalchemy import (sField, sRevision, sTiddler,
        sBag, sRecipe, sUser, sPolicy, sPrincipal, sRole)

def setup_module(module):
    module.store = get_store(config)
    module.environ = {'tiddlyweb.config': config,
            'tiddlyweb.store': module.store}
    session = module.store.storage.session
# delete everything
    for table in (sField, sRevision, sTiddler, sBag, sRecipe, sUser,
            sPolicy, sPrincipal, sRole):
        session.query(table).delete()

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

def test_search_right_revision():
    tiddler = Tiddler('revised', 'bag1')
    tiddler.text = 'alpha'
    tiddler.fields['house'] = 'cottage'
    store.put(tiddler)
    tiddler = Tiddler('revised', 'bag1')
    tiddler.text = 'beta'
    tiddler.fields['house'] = 'mansion'
    store.put(tiddler)
    tiddler = Tiddler('revised', 'bag1')
    tiddler.text = 'gamma'
    tiddler.fields['house'] = 'barn'
    store.put(tiddler)
    tiddler = Tiddler('revised', 'bag1')
    tiddler.text = 'delta'
    tiddler.fields['house'] = 'bungalow'
    store.put(tiddler)
    tiddler = Tiddler('revised', 'bag1')
    tiddler.text = 'epsilon'
    tiddler.fields['house'] = 'treehouse'
    store.put(tiddler)

    tiddlers = list(store.search('beta'))
    assert len(tiddlers) == 0

    tiddlers = list(store.search('epsilon'))
    assert len(tiddlers) == 1
    tiddler = store.get(Tiddler(tiddlers[0].title, tiddlers[0].bag))
    assert tiddler.title == 'revised'
    assert tiddler.bag == 'bag1'
    assert tiddler.fields['house'] == 'treehouse'

    kwords = {'bag': 'bag1', 'house': 'barn'}
    tiddlers = list(index_query(environ, **kwords))

    assert len(tiddlers) == 0

    kwords = {'bag': 'bag1', 'house': 'treehouse'}
    tiddlers = list(index_query(environ, **kwords))

    assert tiddlers[0].title == 'revised'
    assert tiddlers[0].bag == 'bag1'
    assert tiddlers[0].fields['house'] == 'treehouse'

    kwords = {'bag': 'bag1', 'tag': 'orange'}
    tiddlers = list(index_query(environ, **kwords))

    assert len(tiddlers) == 1

    kwords = {'bag': 'bag1', 'tag': 'rang'}
    tiddlers = list(index_query(environ, **kwords))

    assert len(tiddlers) == 0

def test_search_follow_syntax():
    QUERY = 'ftitle:GettingStarted (bag:cdent_public OR bag:fnd_public)'

    store.put(Bag('fnd_public'))
    store.put(Bag('cdent_public'))
    tiddler = Tiddler('GettingStarted', 'fnd_public')
    tiddler.text = 'fnd starts'
    tiddler.fields['house'] = 'treehouse'
    tiddler.fields['car'] = 'porsche'
    store.put(tiddler)
    tiddler = Tiddler('GettingStarted', 'cdent_public')
    tiddler.text = 'cdent starts'
    tiddler.fields['left-hand'] = 'well dirty'
    store.put(tiddler)
    tiddler = Tiddler('other', 'cdent_public')
    tiddler.text = 'cdent starts'
    store.put(tiddler)

    tiddlers = list(store.search('starts'))
    assert len(tiddlers) == 3

    tiddlers = list(store.search(QUERY))
    assert len(tiddlers) == 2

    tiddlers = list(store.search('"cdent starts"'))
    assert len(tiddlers) == 2

    tiddlers = list(store.search('"fnd starts"'))
    assert len(tiddlers) == 1

    tiddler = list(store.search('left-hand:"well dirty"'))
    assert len(tiddlers) == 1

def test_search_arbitrarily_complex():
    QUERY = 'ftitle:GettingStarted (bag:cdent_public OR bag:fnd_public) house:treehouse'

    tiddlers = list(store.search(QUERY))
    for tiddler in tiddlers:
        print tiddler.bag, tiddler.title
    assert len(tiddlers) == 1

    QUERY = 'ftitle:GettingStarted (bag:cdent_public OR bag:fnd_public) house:treehouse car:porsche'

    tiddlers = list(store.search(QUERY))
    for tiddler in tiddlers:
        print tiddler.bag, tiddler.title
    assert len(tiddlers) == 1
