"""
A subclass of tiddlywebplugins.sqlalchemy with mysql specific
tune ups, including a fulltext index and 'indexer' support
for accelerating filters.

http://github.com/cdent/tiddlywebplugins.mysql
http://tiddlyweb.com/

"""
from __future__ import absolute_import

from sqlalchemy.engine import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.expression import and_, or_, text as text_, alias
from sqlalchemy.sql import func

from sqlalchemy.dialects.mysql.base import VARCHAR, LONGTEXT

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.serializer import Serializer
from tiddlyweb.store import StoreError

from tiddlywebplugins.sqlalchemy import (Store as SQLStore,
        sRevision, metadata, Session,
        field_table, revision_table, bag_table, policy_table,
        recipe_table, role_table, user_table)

from tiddlyweb.filters import FilterIndexRefused

from pyparsing import (printables, alphanums, OneOrMore, Group,
        Combine, Suppress, Optional, FollowedBy, Literal, CharsNotIn,
        Word, Keyword, Empty, White, Forward, QuotedString, StringEnd,
        ParseException)

#import logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
#logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)
#logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)

__version__ = '0.9.10'

ENGINE = None
MAPPED = False
TABLES = [field_table, revision_table, bag_table, policy_table, recipe_table,
        role_table, user_table]

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
            metadata.bind = ENGINE
            Session.configure(bind=ENGINE)
        self.session = Session()
        self.serializer = Serializer('text')
        self.parser = DEFAULT_PARSER
        self.producer = Producer()

        if not MAPPED:
            for table in TABLES:
                table.kwargs['mysql_charset'] = 'utf8'
                if table.name == 'revision':
                    for column in table.columns:
                        if column.name == 'tiddler_title':
                            column.type = VARCHAR(length=128,
                                    convert_unicode=True, collation='utf8_bin')
                        if column.name == 'tags':
                            column.type = VARCHAR(length=1024,
                                    convert_unicode=True, collation='utf8_bin')
                        if column.name == 'text':
                            column.type = LONGTEXT(convert_unicode=True,
                                    collation='utf8_bin')
                            
            metadata.create_all(ENGINE)
            MAPPED = True


    def search(self, search_query=''):
        rev_alias = alias(revision_table)
        statement = func.max(rev_alias.c.number)
        statement = statement.select().where(and_(
            sRevision.tiddler_title==rev_alias.c.tiddler_title,
            sRevision.bag_name==rev_alias.c.bag_name))
        query = self.session.query(sRevision.bag_name, sRevision.tiddler_title)
        query = query.filter(sRevision.number==statement)
        try:
            try:
                ast = self.parser(search_query)[0]
                query = self.producer.produce(ast, query)
            except ParseException, exc:
                raise StoreError('failed to parse search query: %s' % exc)

            try:
                for stiddler in query.all():
                    yield Tiddler(unicode(stiddler.tiddler_title),
                            unicode(stiddler.bag_name))
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
    escape = Suppress(escapechar) + (Word(printables, exact=1) | White(exact=1))
    wordtoken = Combine(OneOrMore(wordtext | escape))
# A plain old word.
    plainWord = Group(wordtoken).setResultsName("Word")

# A range of terms
    startfence = Literal("[") | Literal("{")
    endfence = Literal("]") | Literal("}")
    rangeitem = QuotedString('"') | wordtoken
    openstartrange = Group(Empty()) + Suppress(Keyword("TO") + White()) + Group(rangeitem)
    openendrange = Group(rangeitem) + Suppress(White() + Keyword("TO")) + Group(Empty())
    normalrange = Group(rangeitem) + Suppress(White() + Keyword("TO") + White()) + Group(rangeitem)
    range = Group(startfence + (normalrange | openstartrange | openendrange) + endfence).setResultsName("Range")

# A word-like thing
    generalWord = range | plainWord

# A quoted phrase
    quotedPhrase = Group(QuotedString('"')).setResultsName("Quotes")

    expression = Forward()

# Parentheses can enclose (group) any expression
    parenthetical = Group((Suppress("(") + expression + Suppress(")"))).setResultsName("Group")

    boostableUnit = generalWord | quotedPhrase
    boostedUnit = Group(boostableUnit + Suppress("^") + Word("0123456789", ".0123456789")).setResultsName("Boost")

# The user can flag that a parenthetical group, quoted phrase, or word
# should be searched in a particular field by prepending 'fn:', where fn is
# the name of the field.
    fieldableUnit = parenthetical | boostedUnit | boostableUnit
    fieldedUnit = Group(Word(alphanums + "_" + "-") + Suppress(':') + fieldableUnit).setResultsName("Field")

# Units of content
    unit = fieldedUnit | fieldableUnit

# A unit may be "not"-ed.
    operatorNot = Group(Suppress(Keyword("not", caseless=True)) + Suppress(White()) + unit).setResultsName("Not")
    generalUnit = operatorNot | unit

    andToken = Keyword("AND", caseless=False)
    orToken = Keyword("OR", caseless=False)
    andNotToken = Keyword("ANDNOT", caseless=False)

    operatorAnd = Group(generalUnit + OneOrMore(Suppress(White()) + Suppress(andToken) + Suppress(White()) + generalUnit)).setResultsName("And")
    operatorOr = Group(generalUnit + OneOrMore(Suppress(White()) + Suppress(orToken) + Suppress(White()) + generalUnit)).setResultsName("Or")
    operatorAndNot = Group(unit + OneOrMore(Suppress(White()) + Suppress(andNotToken) + Suppress(White()) + unit)).setResultsName("AndNot")

    expression << (OneOrMore(operatorAnd | operatorOr | operatorAndNot | generalUnit | Suppress(White())) | Empty())

    toplevel = Group(expression).setResultsName("Toplevel") + StringEnd()

    return toplevel.parseString

DEFAULT_PARSER = _make_default_parser()

class Producer(object):

    def produce(self, ast, query):
        expressions = self._eval(ast, None) 
        return query.filter(expressions)

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
            if value.endswith('*'):
                value = value.replace('*', '%')
                like = True

            if fieldname == 'ftitle' or fieldname == 'title':
                fieldname = 'tiddler_title'
            if fieldname == 'fbag' or fieldname == 'bag':
                fieldname = 'bag_name'

            if fieldname == 'id':
                bag, title = value.split(':', 1)
                expression = and_(sRevision.bag_name == bag,
                        sRevision.tiddler_title == title)
            elif fieldname == 'tag':
                # XXX: this is insufficiently specific
                expression = sRevision.tags.op('regexp')('(^| {1})%s( {1}|$)'
                        % value)
            elif hasattr(sRevision, fieldname):
                if like:
                    expression = (getattr(sRevision, fieldname).like(value))
                else:
                    expression = (getattr(sRevision, fieldname) == value)
            else:
                sfield_alias = alias(field_table)
                expression = and_(
                        sfield_alias.c.revision_number == sRevision.number,
                        sfield_alias.c.name == fieldname)
                if like:
                    expression = and_(expression,
                            sfield_alias.c.value.like(value))
                else:
                    expression = and_(expression,
                            sfield_alias.c.value == value)
        else:
            expression = (text_(
                'MATCH(revision.tiddler_title, revision.text, revision.tags) '
                + 'AGAINST(:query in boolean mode)')
                .params(query=value))
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
        for subnode in node: 
            expressions.append(self._eval(subnode, fieldname))
        return or_(*expressions)

    def _And(self, node, fieldname):
        expressions = []
        for subnode in node: 
            expressions.append(self._eval(subnode, fieldname))
        return and_(*expressions)

    def _Quotes(self, node, fieldname):
        node[0] = '"%s"' % node[0]
        return self._Word(node, fieldname)
