"""
A subclass of tiddlywebplugins.sqlalchemy with mysql specific
tune ups.
"""
from __future__ import absolute_import

import logging

from sqlalchemy.engine import create_engine
from sqlalchemy.exc import ArgumentError, NoSuchTableError
from sqlalchemy.sql.expression import and_, or_, text as text_, alias
from sqlalchemy.sql import func
from sqlalchemy.schema import Table, PrimaryKeyConstraint
from sqlalchemy.orm import mapper

from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.serializer import Serializer
from tiddlyweb.store import StoreError

from tiddlywebplugins.sqlalchemy import (Store as SQLStore,
        sRevision, metadata, Session,
        field_table, revision_table, bag_table, policy_table,
        recipe_table, principal_table, role_table, user_table)

from tiddlyweb.filters import FilterIndexRefused

from pyparsing import (printables, alphanums, OneOrMore, Group,
        Combine, Suppress, Optional, FollowedBy, Literal, CharsNotIn,
        Word, Keyword, Empty, White, Forward, QuotedString, StringEnd,
        ParseException)

# import logging
# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
# logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)


# class sHead(object):
#     def __repr__(self):
#         return '<sHead(%s:%s:%s)>' % (self.revision_bag_name, self.revision_tiddler_title,
#                 self.head_rev)

ENGINE = None
MAPPED = False
TABLES = [field_table, revision_table, bag_table, policy_table, recipe_table,
        principal_table, role_table, user_table]

class Store(SQLStore):

    def _init_store(self):
        """
        Establish the database engine and session,
        creating tables if needed.
        """
        global ENGINE, MAPPED
        if not ENGINE:
            ENGINE = create_engine(self._db_config(), pool_recycle=3600)
        metadata.bind = ENGINE
        Session.configure(bind=ENGINE)
        self.session = Session()
        self.serializer = Serializer('text')
        self.parser = DEFAULT_PARSER
        self.producer = Producer()

        if not MAPPED:
            for table in TABLES:
                table.kwargs['mysql_engine'] = 'InnoDB'
                table.kwargs['mysql_charset'] = 'utf8'
            metadata.create_all(ENGINE)
            MAPPED = True

    def search(self, search_query=''):
        try:
            query = self.session.query(sRevision,func.max(sRevision.number))
            #query = query.filter(sHead.revision_bag_name == sRevision.bag_name)
            #query = query.filter(sHead.revision_tiddler_title == sRevision.tiddler_title)
            query = query.group_by(sRevision.tiddler_title)

            try:
                ast = self.parser(search_query)[0]
                query = self.producer.produce(ast, query)
            except ParseException, exc:
                raise StoreError('failed to parse search query: %s' % exc)

            return (Tiddler(unicode(stiddler[0].tiddler_title),
                unicode(stiddler[0].bag_name)) for stiddler in query.all())
        except:
            self.session.rollback()
            raise


def index_query(environ, **kwargs):
    store = environ['tiddlyweb.store']

    queries = []
    for key, value in kwargs.items():
        queries.append('%s:"%s"' % (key, value))
    query = ' '.join(queries)
    
    storage = store.storage

    try:
        tiddlers = storage.search(search_query=query)
    except StoreError, exc:
        raise FilterIndexRefused('error in the store: %s' % exc)

    return (store.get(tiddler) for tiddler in tiddlers)


# XXX borrowed from Whoosh
def _make_default_parser():
    escapechar = "\\"

    wordtext = CharsNotIn('\\*?^():"{}[] ')
    escape = Suppress(escapechar) + (Word(printables, exact=1) | White(exact=1))
    wordtoken = Combine(OneOrMore(wordtext | escape))
# A plain old word.
    plainWord = Group(wordtoken).setResultsName("Word")

# A wildcard word containing * or ?.
    wildchars = Word("?*")
# Start with word chars and then have wild chars mixed in
    wildmixed = wordtoken + OneOrMore(wildchars + Optional(wordtoken))
# Or, start with wildchars, and then either a mixture of word and wild chars, or the next token
    wildstart = wildchars + (OneOrMore(wordtoken + Optional(wildchars)) | FollowedBy(White() | StringEnd()))
    wildcard = Group(Combine(wildmixed | wildstart)).setResultsName("Wildcard")

# A range of terms
    startfence = Literal("[") | Literal("{")
    endfence = Literal("]") | Literal("}")
    rangeitem = QuotedString('"') | wordtoken
    openstartrange = Group(Empty()) + Suppress(Keyword("TO") + White()) + Group(rangeitem)
    openendrange = Group(rangeitem) + Suppress(White() + Keyword("TO")) + Group(Empty())
    normalrange = Group(rangeitem) + Suppress(White() + Keyword("TO") + White()) + Group(rangeitem)
    range = Group(startfence + (normalrange | openstartrange | openendrange) + endfence).setResultsName("Range")

# A word-like thing
    generalWord = range | wildcard | plainWord

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

    operatorAnd = Group(generalUnit + Suppress(White()) + Suppress(andToken) + Suppress(White()) + expression).setResultsName("And")
    operatorOr = Group(generalUnit + Suppress(White()) + Suppress(orToken) + Suppress(White()) + expression).setResultsName("Or")
    operatorAndNot = Group(unit + Suppress(White()) + Suppress(andNotToken) + Suppress(White()) + unit).setResultsName("AndNot")

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
        if fieldname:
            if fieldname == 'ftitle' or fieldname == 'title':
                fieldname = 'tiddler_title'
            if fieldname == 'fbag' or fieldname == 'bag':
                fieldname = 'bag_name'

            if fieldname == 'id':
                bag, title = node[0].split(':', 1)
                expression = and_(sRevision.bag_name == bag,
                        sRevision.tiddler_title == title)
            elif fieldname == 'tag':
                # XXX: this is insufficiently specific
                expression = sRevision.tags.op('regexp')('(^| {1})%s( {1}|$)'
                        % node[0])
            elif hasattr(sRevision, fieldname):
                expression = (getattr(sRevision, fieldname) == node[0])
            else:
                sfield_alias = alias(field_table)
                expression = and_(sfield_alias.c.tiddler_title == sRevision.tiddler_title,
                        sfield_alias.c.bag_name == sRevision.bag_name,
                        sfield_alias.c.revision_number == sRevision.number,
                        sfield_alias.c.name == fieldname,
                        sfield_alias.c.value == node[0])
        else:
            expression = (text_('MATCH(revision.tiddler_title, text, tags) '
                + 'AGAINST(:query in boolean mode)')
                .params(query=node[0]))
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
