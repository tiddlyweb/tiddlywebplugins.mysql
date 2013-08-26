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

from sqlalchemy import event
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import DisconnectionError

from sqlalchemy.dialects.mysql.base import VARCHAR, LONGTEXT

from tiddlywebplugins.sqlalchemy3 import (Store as SQLStore, Base, Session,
        index_query)

import logging

#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
#logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)

__version__ = '3.1.1'

ENGINE = None
MAPPED = False


LOGGER = logging.getLogger(__name__)


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
            LOGGER.debug('got mysql server has gone away: %s', ex)
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
        super(Store, self).__init__(store_config, environ)
        self.has_geo = True

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
            _map_tables(self.environ['tiddlyweb.config'],
                    Base.metadata.sorted_tables)
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


def _map_tables(config, tables):
    """
    Transform the sqlalchemy table information into mysql specific
    table information.

    XXX: This ought to be doable in a more declarative fashion.
    """
    fulltext = config.get('mysql.fulltext', False)
    for table in tables:

        if table.name == 'text' and fulltext:
            table.kwargs['mysql_engine'] = 'MyISAM'
        else:
            table.kwargs['mysql_engine'] = 'InnoDB'

        if table.name == 'revision' or table.name == 'tiddler':
            for column in table.columns:
                if (column.name == 'tiddler_title'
                        or column.name == 'title'):
                    column.type = VARCHAR(length=128, convert_unicode=True)

        if table.name == 'text':
            for column in table.columns:
                if column.name == 'text':
                    column.type = LONGTEXT(convert_unicode=True)

        if table.name == 'tag':
            for column in table.columns:
                if column.name == 'tag':
                    column.type = VARCHAR(length=191, convert_unicode=True)

        if table.name == 'field':
            for index in table.indexes:
                # XXX: is the naming system reliable?
                if index.name == 'ix_field_value':
                    index.kwargs['mysql_length'] = 191
