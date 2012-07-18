"""
A subclass of tiddlywebplugins.sqlalchemy with mysql specific
tune ups, including a fulltext index and 'indexer' support
for accelerating filters.

http://github.com/cdent/tiddlywebplugins.mysql
http://tiddlyweb.com/

"""
from __future__ import absolute_import, with_statement

import warnings
import MySQLdb

from pyparsing import ParseException

from sqlalchemy import event
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import ProgrammingError, DisconnectionError

from sqlalchemy.dialects.mysql.base import VARCHAR, LONGTEXT

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.serializer import Serializer
from tiddlyweb.store import StoreError

from tiddlywebplugins.sqlalchemy3 import (Store as SQLStore,
        sField, sTag, sText, sTiddler, sRevision, Base, Session)

from tiddlyweb.filters import FilterIndexRefused

from .parser import DEFAULT_PARSER
from .producer import Producer

import logging

#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
#logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)

__version__ = '3.0.5'

ENGINE = None
MAPPED = False


def on_checkout(dbapi_con, con_record, con_proxy):
    """
    Ensures that MySQL connections checked out of the
    pool are alive.

    Borrowed from:
    http://groups.google.com/group/sqlalchemy/msg/a4ce563d802c929f
    """
    try:
        try:
            dbapi_con.ping(False)
        except TypeError:
            dbapi_con.ping()
    except dbapi_con.OperationalError, ex:
        if ex.args[0] in (2006, 2013, 2014, 2045, 2055):
            logging.debug('got mysql server has gone away: %s', ex)
            # caught by pool, which will retry with a new connection
            raise DisconnectionError()
        else:
            raise


class Store(SQLStore):
    """
    An adaptation of the generic sqlalchemy store, to add mysql
    specific functionality, including search.
    """

    def __init__(self, store_config=None, environ=None):
        self.serializer = Serializer('text')
        self.parser = DEFAULT_PARSER
        self.producer = Producer()
        SQLStore.__init__(self, store_config, environ)

    def _init_store(self):
        """
        Establish the database engine and session,
        creating tables if needed.
        """
        global ENGINE, MAPPED
        if not ENGINE:
            ENGINE = create_engine(self._db_config(),
                    pool_recycle=3600,
                    pool_size=20,
                    max_overflow=-1,
                    pool_timeout=2)
            event.listen(ENGINE, 'checkout', on_checkout)
            Base.metadata.bind = ENGINE
            Session.configure(bind=ENGINE)
        self.session = Session()

        if not MAPPED:
            _map_tables(Base.metadata.sorted_tables)
            Base.metadata.create_all(ENGINE)
            MAPPED = True

    def tiddler_put(self, tiddler):
        """
        Override the super to trap MySQLdb.Warning which is raised
        when mysqld would truncate a field during an insert. We
        want to not store the tiddler, and report a useful error.
        """
        warnings.simplefilter('error', MySQLdb.Warning)
        try:
            SQLStore.tiddler_put(self, tiddler)
        except MySQLdb.Warning, exc:
            raise TypeError('mysql refuses to store tiddler: %s' % exc)

    def search(self, search_query=''):
        """
        Do a search of of the database, using the 'q' query,
        parsed by the parser and turned into a producer.
        """
        query = self.session.query(sTiddler).join('current')
        if '_limit:' not in search_query:
            default_limit = self.environ.get(
                    'tiddlyweb.config', {}).get(
                            'mysql.search_limit', '20')
            search_query += ' _limit:%s' % default_limit
        try:
            try:
                ast = self.parser(search_query)[0]
                query = self.producer.produce(ast, query)
            except ParseException, exc:
                raise StoreError('failed to parse search query: %s' % exc)

            try:
                for stiddler in query.all():
                    try:
                        yield Tiddler(unicode(stiddler.title),
                                unicode(stiddler.bag))
                    except AttributeError:
                        stiddler = stiddler[0]
                        yield Tiddler(unicode(stiddler.title),
                                unicode(stiddler.bag))
                self.session.close()
            except ProgrammingError, exc:
                raise StoreError('generated search SQL incorrect: %s' % exc)
        except:
            self.session.rollback()
            raise


def index_query(environ, **kwargs):
    """
    Attempt to optimize filter processing by using the search index
    to provide results that can be matched.

    In practice, this proves not to be that helpful when memcached
    is being used, but it is in other situations.
    """
    store = environ['tiddlyweb.store']

    queries = []
    for key, value in kwargs.items():
        if '"' in value:
            # XXX The current parser is currently unable to deal with
            # nested quotes. Rather than running the risk of tweaking
            # the parser with unclear results, we instead just refuse
            # for now. Later this can be fixed for real.
            raise FilterIndexRefused('unable to process values with quotes')
        queries.append('%s:"%s"' % (key, value))
    query = ' '.join(queries)

    storage = store.storage

    try:
        tiddlers = storage.search(search_query=query)
        return (store.get(tiddler) for tiddler in tiddlers)
    except StoreError, exc:
        raise FilterIndexRefused('error in the store: %s' % exc)


def _map_tables(tables):
    """
    Transform the sqlalchemy table information into mysql specific
    table information.

    XXX: This ought to be doable in a more declarative fashion.
    """
    for table in tables:
        table.kwargs['mysql_charset'] = 'utf8'

        if table.name == 'text':
            table.kwargs['mysql_engine'] = 'MyISAM'
        else:
            table.kwargs['mysql_engine'] = 'InnoDB'

        if table.name == 'revision' or table.name == 'tiddler':
            for column in table.columns:
                if (column.name == 'tiddler_title'
                        or column.name == 'title'):
                    column.type = VARCHAR(length=128,
                            convert_unicode=True, collation='utf8_bin')

        if table.name == 'text':
            for column in table.columns:
                if column.name == 'text':
                    column.type = LONGTEXT(convert_unicode=True,
                            collation='utf8_bin')

        if table.name == 'tag':
            for column in table.columns:
                if column.name == 'tag':
                    column.type = VARCHAR(length=191,
                            convert_unicode=True, collation='utf8_bin')

        if table.name == 'field':
            for column in table.columns:
                if column.name == 'value':
                    column.type = VARCHAR(length=191,
                            convert_unicode=True, collation='utf8_bin')
