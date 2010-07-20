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

from tiddlywebplugins.sqlalchemy import (Store as SQLStore,
        sTiddler, sRevision, metadata, field_table)

from tiddlyweb.filters import FilterIndexRefused

from whoosh.qparser.default import QueryParser

import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)


class sHead(object):
    def __repr__(self):
        return '<sHead(%s:%s:%s)>' % (self.revision_bag_name, self.revision_tiddler_title,
                self.head_rev)


class Store(SQLStore):

    def _init_store(self):
        """
        Establish the database engine and session,
        creating tables if needed.
        """
        SQLStore._init_store(self)

        self.parser = DEFAULT_PARSER
        self.producer = Producer()

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

        ast = self.parser(search_query)[0]
        query = self.producer.produce(ast, query)

        return (Tiddler(unicode(stiddler.tiddler_title),
            unicode(stiddler.bag_name)) for stiddler in query.all())


def index_query(environ, **kwargs):
    store = environ['tiddlyweb.store']

    queries = []
    for key, value in kwargs.items():
        queries.append('%s:"%s"' % (key, value))
    query = ' '.join(queries)
    
    storage = store.storage

    tiddlers = storage.search(search_query=query)

    return (store.get(tiddler) for tiddler in tiddlers)


from pyparsing import (printables, alphanums, OneOrMore, Group,
        Combine, Suppress, Optional, FollowedBy, Literal, CharsNotIn,
        Word, Keyword, Empty, White, Forward, QuotedString, StringEnd)


# XXX borrowed from Whoosh
def _make_default_parser():
    escapechar = "\\"

#wordchars = printables
#for specialchar in '*?^():"{}[] ' + escapechar:
#    wordchars = wordchars.replace(specialchar, "")
#wordtext = Word(wordchars)

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
    fieldedUnit = Group(Word(alphanums + "_") + Suppress(':') + fieldableUnit).setResultsName("Field")

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
                fieldname = 'revision_tiddler_title'
            if fieldname == 'fbag' or fieldname == 'bag':
                fieldname = 'revision_bag_name'

            if fieldname == 'id':
                bag, title = node[0].split(':', 1)
                expression = and_(sHead.revision_bag_name == bag,
                        sHead.revision_tiddler_title == title)
            elif fieldname == 'tag':
                # XXX: this is insufficiently specific
                expression = sRevision.tags.op('regexp')('(^| {1})%s( {1}|$)'
                        % node[0])
            elif hasattr(sHead, fieldname):
                expression = (getattr(sHead, fieldname) == node[0])
            elif hasattr(sRevision, fieldname):
                expression = (getattr(sRevision, fieldname) == node[0])
            else:
                sfield_alias = alias(field_table)
                expression = and_(sfield_alias.c.tiddler_title == sHead.revision_tiddler_title,
                        sfield_alias.c.bag_name == sHead.revision_bag_name,
                        sfield_alias.c.revision_number == sHead.head_rev,
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
        return self._Word(node, fieldname)
