"""
A subclass of tiddlywebplugins.sqlalchemy with mysql specific
tune ups, including a fulltext index and 'indexer' support
for accelerating filters.

http://github.com/cdent/tiddlywebplugins.mysql
http://tiddlyweb.com/

"""
from __future__ import absolute_import

from sqlalchemy import event
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import ProgrammingError, DisconnectionError
from sqlalchemy.orm import aliased
from sqlalchemy.orm.exc import NoResultFound, StaleDataError
from sqlalchemy.sql.expression import (and_, or_, not_, text as text_, label)
from sqlalchemy.sql import func

from sqlalchemy.dialects.mysql.base import VARCHAR, LONGTEXT

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.serializer import Serializer
from tiddlyweb.store import StoreError, NoTiddlerError

from tiddlywebplugins.sqlalchemy3 import (Store as SQLStore,
        sField, sTag, sText, sTiddler, sRevision, Base, Session)

from tiddlyweb.filters import FilterIndexRefused

from pyparsing import (printables, alphanums, OneOrMore, Group,
        Combine, Suppress, Literal, CharsNotIn,
        Word, Keyword, Empty, White, Forward, QuotedString, StringEnd,
        ParseException)

import logging

#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
#logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)

__version__ = '3.0.1'

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

    def _init_store(self):
        """
        Establish the database engine and session,
        creating tables if needed.
        """
        global ENGINE, MAPPED
        if not ENGINE:
            ENGINE = create_engine(self._db_config(),
                    pool_recycle=3600,
                    pool_size=20,  # XXX these three ought to come from config
                    max_overflow=-1,
                    pool_timeout=2)
            event.listen(ENGINE, 'checkout', on_checkout)
            Base.metadata.bind = ENGINE
            Session.configure(bind=ENGINE)
        self.session = Session()
        self.serializer = Serializer('text')
        self.parser = DEFAULT_PARSER
        self.producer = Producer()

        if not MAPPED:
            for table in Base.metadata.sorted_tables:
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

            Base.metadata.create_all(ENGINE)
            MAPPED = True

    def search(self, search_query=''):
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


# XXX borrowed from Whoosh
def _make_default_parser():
    escapechar = "\\"

    wordtext = CharsNotIn('\\():"{}[] ')
    escape = Suppress(escapechar) + (Word(printables, exact=1)
            | White(exact=1))
    wordtoken = Combine(OneOrMore(wordtext | escape))
# A plain old word.
    plainWord = Group(wordtoken).setResultsName("Word")

# A range of terms
    startfence = Literal("[") | Literal("{")
    endfence = Literal("]") | Literal("}")
    rangeitem = QuotedString('"') | wordtoken
    openstartrange = Group(Empty()) + Suppress(Keyword("TO")
            + White()) + Group(rangeitem)
    openendrange = Group(rangeitem) + Suppress(White()
            + Keyword("TO")) + Group(Empty())
    normalrange = Group(rangeitem) + Suppress(White()
            + Keyword("TO") + White()) + Group(rangeitem)
    range = Group(startfence + (normalrange | openstartrange
        | openendrange) + endfence).setResultsName("Range")

# A word-like thing
    generalWord = range | plainWord

# A quoted phrase
    quotedPhrase = Group(QuotedString('"')).setResultsName("Quotes")

    expression = Forward()

# Parentheses can enclose (group) any expression
    parenthetical = Group((Suppress("(") + expression
        + Suppress(")"))).setResultsName("Group")

    boostableUnit = generalWord | quotedPhrase
    boostedUnit = Group(boostableUnit + Suppress("^")
            + Word("0123456789", ".0123456789")).setResultsName("Boost")

# The user can flag that a parenthetical group, quoted phrase, or word
# should be searched in a particular field by prepending 'fn:', where fn is
# the name of the field.
    fieldableUnit = parenthetical | boostedUnit | boostableUnit
    fieldedUnit = Group(Word(alphanums + "_" + "-"
        + ".") + Suppress(':') + fieldableUnit).setResultsName("Field")

# Units of content
    generalUnit = fieldedUnit | fieldableUnit

    andToken = Keyword("AND", caseless=False)
    orToken = Keyword("OR", caseless=False)
    notToken = Keyword("NOT", caseless=False)

    operatorAnd = Group(generalUnit + OneOrMore(
        Suppress(White()) + Suppress(andToken) + Suppress(White())
        + generalUnit)).setResultsName("And")
    operatorOr = Group(generalUnit + OneOrMore(
        Suppress(White()) + Suppress(orToken) + Suppress(White())
        + generalUnit)).setResultsName("Or")
    operatorNot = Group(Suppress(notToken) + Suppress(White()) +
        generalUnit).setResultsName("Not")

    expression << (OneOrMore(operatorAnd | operatorOr | operatorNot
        | generalUnit | Suppress(White())) | Empty())

    toplevel = Group(expression).setResultsName("Toplevel") + StringEnd()

    return toplevel.parseString

DEFAULT_PARSER = _make_default_parser()


class Producer(object):

    def produce(self, ast, query):
        self.joined_revision = False
        self.joined_tags = False
        self.joined_fields = False
        self.joined_text = False
        self.in_and = False
        self.in_or = False
        self.in_not = False
        self.query = query
        self.limit = None
        expressions = self._eval(ast, None)
        if self.limit:
            return self.query.filter(expressions).limit(self.limit)
        else:
            return self.query.filter(expressions)

    def _eval(self, node, fieldname):
        name = node.getName()
        return getattr(self, "_" + name)(node, fieldname)

    def _Toplevel(self, node, fieldname):
        expressions = []
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        return and_(*expressions)

    def _Word(self, node, fieldname):
        value = node[0]
        if fieldname:
            like = False
            try:
                if value.endswith('*'):
                    value = value.replace('*', '%')
                    like = True
            except TypeError:
                # Hack around field values containing parens
                # The node[0] is a non-string if that's the case.
                node[0] = '(' + value[0] + ')'
                return self._Word(node, fieldname)

            if fieldname == 'ftitle':
                fieldname = 'title'
            if fieldname == 'fbag':
                fieldname = 'bag'

            if fieldname == 'bag':
                if like:
                    expression = (sTiddler.bag.like(value))
                else:
                    expression = (sTiddler.bag == value)
            elif fieldname == 'title':
                if like:
                    expression = (sTiddler.title.like(value))
                else:
                    expression = (sTiddler.title == value)
            elif fieldname == 'id':
                bag, title = value.split(':', 1)
                expression = and_(sTiddler.bag == bag,
                        sTiddler.title == title)
            elif fieldname == 'tag':
                if self.in_and:
                    tag_alias = aliased(sTag)
                    self.query = self.query.join(tag_alias)
                    if like:
                        expression = (tag_alias.tag.like(value))
                    else:
                        expression = (tag_alias.tag == value)
                else:
                    if not self.joined_tags:
                        self.query = self.query.join(sTag)
                        if like:
                            expression = (sTag.tag.like(value))
                        else:
                            expression = (sTag.tag == value)
                        self.joined_tags = True
                    else:
                        if like:
                            expression = (sTag.tag.like(value))
                        else:
                            expression = (sTag.tag == value)
            elif fieldname == 'near':
                # proximity search on geo.long, geo.lat based on
                # http://cdent.tiddlyspace.com/bags/cdent_public/tiddlers/Proximity%20Search.html
                try:
                    lat, long, radius = [float(item)
                            for item in value.split(',', 2)]
                except ValueError, exc:
                    raise StoreError(
                            'failed to parse search query, malformed near: %s'
                            % exc)
                field_alias1 = aliased(sField)
                field_alias2 = aliased(sField)
                distance = label(u'greatcircle', (6371000
                    * func.acos(
                        func.cos(
                            func.radians(lat))
                        * func.cos(
                            func.radians(field_alias2.value))
                        * func.cos(
                            func.radians(field_alias1.value)
                            - func.radians(long))
                        + func.sin(
                            func.radians(lat))
                        * func.sin(
                            func.radians(field_alias2.value)))))
                self.query = self.query.add_columns(distance)
                self.query = self.query.join(field_alias1)
                self.query = self.query.join(field_alias2)
                self.query = self.query.having(
                        u'greatcircle < %s' % radius).order_by('greatcircle')
                expression = and_(field_alias1.name == u'geo.long',
                        field_alias2.name == u'geo.lat')
                self.limit = 20 # XXX: make this passable
            elif fieldname == '_limit':
                try:
                    self.limit = int(value)
                except ValueError:
                    pass
                self.query = self.query.order_by(
                        sRevision.modified.desc())
                expression = None
            elif hasattr(sRevision, fieldname):
                if self.in_and:
                    revision_alias = aliased(sRevision)
                    self.query = self.query.join(revision_alias)
                    if like:
                        expression = (getattr(revision_alias,
                            fieldname).like(value))
                    else:
                        expression = (getattr(revision_alias,
                            fieldname) == value)
                else:
                    if like:
                        expression = (getattr(sRevision,
                            fieldname).like(value))
                    else:
                        expression = (getattr(sRevision,
                            fieldname) == value)
            else:
                if self.in_and:
                    field_alias = aliased(sField)
                    self.query = self.query.join(field_alias)
                    expression = (field_alias.name == fieldname)
                    if like:
                        expression = and_(expression,
                                field_alias.value.like(value))
                    else:
                        expression = and_(expression,
                                field_alias.value == value)
                else:
                    if not self.joined_fields:
                        self.query = self.query.join(sField)
                        expression = (sField.name == fieldname)
                        if like:
                            expression = and_(expression,
                                    sField.value.like(value))
                        else:
                            expression = and_(expression,
                                    sField.value == value)
                        self.joined_fields = True
                    else:
                        expression = (sField.name == fieldname)
                        if like:
                            expression = and_(expression,
                                    sField.value.like(value))
                        else:
                            expression = and_(expression,
                                    sField.value == value)
        else:
            if not self.joined_text:
                self.query = self.query.join(sText)
                self.joined_text = True
            expression = (text_(
                'MATCH(text.text) '
                + "AGAINST('%s' in boolean mode)" % value))
        return expression

    def _Field(self, node, fieldname):
        return self._Word(node[1], node[0])

    def _Group(self, node, fieldname):
        expressions = []
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        return and_(*expressions)

    def _Or(self, node, fieldname):
        expressions = []
        self.in_or = True
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        self.in_or = False
        return or_(*expressions)

    def _And(self, node, fieldname):
        expressions = []
        self.in_and = True
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        self.in_and = False
        return and_(*expressions)

    def _Not(self, node, fieldname):
        expressions = []
        self.in_not = True
        for subnode in node:
            expressions.append(self._eval(subnode, fieldname))
        self.in_not = False
        return not_(*expressions)

    def _Quotes(self, node, fieldname):
        node[0] = '"%s"' % node[0]
        return self._Word(node, fieldname)
