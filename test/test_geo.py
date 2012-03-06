from tiddlyweb.config import config

from tiddlyweb.store import StoreError

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.bag import Bag

from tiddlywebplugins.utils import get_store

from tiddlywebplugins.mysql3 import index_query, Base

import py.test

def setup_module(module):
    module.store = get_store(config)
    module.environ = {'tiddlyweb.config': config,
            'tiddlyweb.store': module.store}
    session = module.store.storage.session
# delete everything
    Base.metadata.drop_all()
    Base.metadata.create_all()

def test_simple_store():
    bag = Bag('bag1')
    store.put(bag)
    tiddler = Tiddler('place1', 'bag1')
    tiddler.text = u'someplace nice'
    tiddler.tags = [u'toilet']
    tiddler.fields[u'geo.lat'] = u'10.5'
    tiddler.fields[u'geo.long'] = u'-10.5'
    store.put(tiddler)

    retrieved = Tiddler('place1', 'bag1')
    retrieved = store.get(retrieved)

    assert retrieved.text == tiddler.text

    tiddler = Tiddler('not a place', 'bag1')
    tiddler.text = u'no where nice'
    store.put(tiddler)

def test_geo_search_find():
    # find things near 10,-10 radius 100 km
    tiddlers = list(store.search(u'near:10,-10,100000'))
    assert len(tiddlers) == 1
    tiddler = store.get(tiddlers[0])
    assert tiddler.title == 'place1'
    assert tiddler.fields['geo.lat'] == '10.5'
    assert tiddler.fields['geo.long'] == '-10.5'

def test_geo_search_not_find():
    # don't find things when we are far away
    tiddlers = list(store.search(u'near:60,-60,100000'))
    assert len(tiddlers) == 0

def test_geo_bad_input():
    py.test.raises(StoreError,
            'list(store.search(u"near:60,-60,select barney from users"))')
    py.test.raises(StoreError,
            'list(store.search(u"near:60,-60,3km"))')

def test_find_toilet():
    tiddlers = list(store.search(u'near:10,-10,100000 tag:toilet'))
    assert len(tiddlers) == 1

    # remove the toilet tag
    tiddler = Tiddler('place1', 'bag1')
    tiddler = store.get(tiddler)
    tiddler.tags = []
    store.put(tiddler)

    tiddlers = list(store.search(u'near:10,-10,100000 tag:toilet'))
    assert len(tiddlers) == 0
