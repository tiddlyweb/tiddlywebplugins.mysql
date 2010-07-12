"""
A subclass of tiddlywebplugins.sqlalchemy with mysql specific
tune ups.
"""

from tiddlyweb.model.tiddler import Tiddler

from tiddlywebplugins.sqlalchemy import (Store as SQLStore,
        sTiddler, sRevision, sField)

from tiddlyweb.filters import FilterIndexRefused


class Store(SQLStore):

    def search(self, search_query=''):
        query = self.session.query(sTiddler)
        query = query.filter(
               'MATCH(title, text, tags) AGAINST(:query in boolean mode)'
                ).params(query=search_query)
        return (Tiddler(unicode(stiddler.title),
            unicode(stiddler.bag_name)) for stiddler in query.all())


def index_query(environ, **kwargs):
    store = environ['tiddlyweb.store']
    try:
        session = store.storage.session
    except (AttributeError, KeyError):
        raise FilterIndexRefused

    id = kwargs.get('id')
    if id:
        bag_name, title = id.split(':', 1)
        kwargs['bag_name'] = bag_name
        kwargs['title'] = title
        del kwargs['id']

    if 'bag' in kwargs:
        kwargs['bag_name'] = kwargs['bag']
        del kwargs['bag']

    query = session.query(sTiddler)
    for field in kwargs.keys():
        if field == 'tag':
            query = (query.filter(sRevision.tags.like('%%%s%%' %
                kwargs['tag'])))
        elif hasattr(sTiddler, field):
            query = (query.filter(getattr(sTiddler,
                field) == kwargs[field]))
        else:
            query = (query.filter(sField.name == field).
                    filter(sField.value == kwargs[field]))

    return (store.get(Tiddler(unicode(stiddler.title),
        unicode(stiddler.bag_name))) for stiddler in query.all())
