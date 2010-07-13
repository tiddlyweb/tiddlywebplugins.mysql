"""
A subclass of tiddlywebplugins.sqlalchemy with mysql specific
tune ups.
"""
from __future__ import absolute_import

import logging

from sqlalchemy.engine import create_engine
from sqlalchemy.exc import ArgumentError, NoSuchTableError
from sqlalchemy.sql import func
from sqlalchemy.schema import Table, PrimaryKeyConstraint
from sqlalchemy.orm import mapper

from tiddlyweb.model.tiddler import Tiddler

from tiddlywebplugins.sqlalchemy import (Store as SQLStore,
        sTiddler, sRevision, sField, metadata)

from tiddlyweb.filters import FilterIndexRefused


class sHead(object):
    def __repr__(self):
        return '<sHead(%s:%s:%s:%s)>' % (self.bag_name, self.tiddler_title,
                self.number)


class Store(SQLStore):

    def _init_store(self):
        """
        Establish the database engine and session,
        creating tables if needed.
        """
        SQLStore._init_store(self)

        try:
            head_table = self._load_head_table()
        except NoSuchTableError:
            self.session.execute("""
CREATE VIEW head
  AS SELECT revision.bag_name AS revision_bag_name,
    revision.tiddler_title AS revision_tiddler_title,
    max(revision.number) AS head_rev
  FROM revision GROUP BY revision.bag_name, revision.tiddler_title;
""")
            head_table = self._load_head_table()

        try:
            mapper(sHead, head_table)
        except ArgumentError, exc:
            logging.debug('sHead already mapped: %s', exc)


    def _load_head_table(self):
        engine = self.session.connection()
        return Table('head', metadata,
                PrimaryKeyConstraint('revision_bag_name', 'revision_tiddler_title'),
                autoload_with=engine,
                useexisting=True,
                autoload=True)

    def search(self, search_query=''):
        query = self.session.query(sRevision)
        query = query.filter(sHead.revision_bag_name == sRevision.bag_name)
        query = query.filter(sHead.revision_tiddler_title == sRevision.tiddler_title)
        query = query.filter(sHead.head_rev == sRevision.number)
        query = (query.filter(
               'MATCH(revision.tiddler_title, text, tags) AGAINST(:query in boolean mode)'
                )
                .params(query=search_query)
                )
        return (Tiddler(unicode(stiddler.tiddler_title),
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
        kwargs['tiddler_title'] = title
        del kwargs['id']

    if 'bag' in kwargs:
        kwargs['bag_name'] = kwargs['bag']
        del kwargs['bag']

    query = session.query(sRevision)
    query = query.filter(sHead.revision_bag_name == sRevision.bag_name)
    query = query.filter(sHead.revision_tiddler_title == sRevision.tiddler_title)
    query = query.filter(sHead.head_rev == sRevision.number)
    query = query.filter(sHead.head_rev == sField.revision_number)
    for field in kwargs.keys():
        if field == 'tag':
            # XXX: this is insufficiently specific
            query = (query.filter(sRevision.tags.like('%%%s%%' %
                kwargs['tag'])))
        elif hasattr(sRevision, field):
            query = (query.filter(getattr(sRevision,
                field) == kwargs[field]))
        else:
            query = (query.filter(sField.name == field).
                    filter(sField.value == kwargs[field]))

    return (store.get(Tiddler(unicode(stiddler.tiddler_title),
        unicode(stiddler.bag_name))) for stiddler in query.all())
