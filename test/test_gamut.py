
import os

import py.test

from tiddlyweb.config import config
from tiddlyweb.store import Store, NoBagError, NoUserError, NoRecipeError, NoTiddlerError

from tiddlyweb.model.bag import Bag
from tiddlyweb.model.recipe import Recipe
from tiddlyweb.model.tiddler import Tiddler
from tiddlyweb.model.user import User

from tiddlywebplugins.mysql3 import Base

from base64 import b64encode

#RANGE = 1000
RANGE = 10

def setup_module(module):
    module.store = Store(
            config['server_store'][0],
            config['server_store'][1],
            {'tiddlyweb.config': config}
            )
# delete everything
    Base.metadata.drop_all()
    Base.metadata.create_all()
    import warnings
    warnings.simplefilter('error')


def test_make_a_bunch():
    for x in xrange(RANGE):
        bag_name = u'bag%s' % x
        recipe_name = u'recipe%s' % x
        tiddler_name = u'tiddler%s' % x
        recipe_list = [(bag_name, '')]
        tiddler_text = u'hey ho %s' % x
        field_name = u'field%s' % x
        field_name2 = u'fieldone%s' % x
        tag_name = u'tag%s' % x
        user_name = u'user%s' % x
        user_pass = u'pass%s' % x
        user_note = u'note%s' % x
        user_roles = [u'rolehold', u'role%s' % x]

        bag = Bag(bag_name)
        bag.policy.owner = u'owner%s' % x
        bag.policy.read = [u'hi%s' % x, u'andextra']
        bag.policy.manage = [u'R:hi%s' % x, u'andmanage']
        store.put(bag)
        recipe = Recipe(recipe_name)
        recipe.policy.owner = u'owner%s' % x
        recipe.policy.read = [u'hi%s' % x, u'andextra']
        recipe.policy.manage = [u'R:hi%s' % x, u'andmanage']
        recipe.set_recipe(recipe_list)
        store.put(recipe)
        tiddler = Tiddler(tiddler_name, bag_name)
        tiddler.text = tiddler_text
        tiddler.fields[field_name] = field_name
        tiddler.fields[field_name2] = field_name2
        tiddler.fields['server.host'] = 'gunky'
        tiddler.tags = [tag_name]
        store.put(tiddler)
        store.put(tiddler)
        user = User(user_name)
        user.set_password(user_pass)
        user.note = user_note
        for role in user_roles:
            user.add_role(role)
        store.put(user)

    bags = [bag.name for bag in store.list_bags()]
    recipes = [recipe.name for recipe in store.list_recipes()]
    users = [user.usersign for user in store.list_users()]
    assert len(bags) == RANGE
    assert len(recipes) == RANGE
    assert len(users) == RANGE
    for x in xrange(RANGE):
        bname = 'bag%s' % x
        rname = 'recipe%s' % x
        uname = 'user%s' % x
        assert bname in bags
        assert rname in recipes
        assert uname in users

    tiddler = store.get(Tiddler('tiddler0', 'bag0'))
    assert tiddler.fields['field0'] == 'field0'
    assert tiddler.fields['fieldone0'] == 'fieldone0'

    bag = Bag('bag0')
    bag = store.get(bag)
    tiddlers = []
    for tiddler in store.list_bag_tiddlers(bag):
        tiddlers.append(store.get(tiddler))
    assert len(tiddlers) == 1
    assert tiddlers[0].title == 'tiddler0'
    assert tiddlers[0].fields['field0'] == 'field0'
    assert tiddlers[0].fields['fieldone0'] == 'fieldone0'
    assert tiddlers[0].tags == ['tag0']
    assert sorted(bag.policy.read) == ['andextra', 'hi0']
    assert sorted(bag.policy.manage) == ['R:hi0', u'andmanage']
    assert bag.policy.owner == 'owner0'

    user = User('user1')
    user = store.get(user)
    assert user.usersign == 'user1'
    assert user.check_password('pass1')
    assert user.note == 'note1'
    assert 'role1' in user.list_roles()
    assert 'rolehold' in user.list_roles()

    recipe = Recipe('recipe2')
    recipe = store.get(recipe)
    assert recipe.name == 'recipe2'
    bags = [bag_name for bag_name, filter in recipe.get_recipe()]
    assert len(bags) == 1
    assert 'bag2' in bags
    assert sorted(recipe.policy.read) == ['andextra', 'hi2']
    assert sorted(recipe.policy.manage) == ['R:hi2', u'andmanage']
    assert recipe.policy.owner == 'owner2'

    recipe.policy.manage = [u'andmanage']
    store.put(recipe)

    recipe = Recipe ('recipe2')
    recipe = store.get(recipe)
    assert recipe.policy.manage == [u'andmanage']

    # delete the above things
    store.delete(bag)
    py.test.raises(NoBagError, 'store.delete(bag)')
    py.test.raises(NoBagError, 'store.get(bag)')
    store.delete(recipe)
    py.test.raises(NoRecipeError, 'store.delete(recipe)')
    py.test.raises(NoRecipeError, 'store.get(recipe)')
    store.delete(user)
    py.test.raises(NoUserError, 'store.delete(user)')
    py.test.raises(NoUserError, 'store.get(user)')

    tiddler = Tiddler('tiddler9', 'bag9')
    store.get(tiddler)
    assert tiddler.bag == 'bag9'
    assert tiddler.text == 'hey ho 9'
    assert tiddler.tags == ['tag9']
    assert tiddler.fields['field9'] == 'field9'
    assert 'server.host' not in tiddler.fields
    store.delete(tiddler)
    py.test.raises(NoTiddlerError, 'store.delete(tiddler)')
    py.test.raises(NoTiddlerError, 'store.get(tiddler)')

def test_binary_tiddler():
    tiddler = Tiddler('binary', 'bag8')
    tiddler.type = 'application/binary'
    tiddler.text = 'not really binary'
    store.put(tiddler)

    new_tiddler = Tiddler('binary', 'bag8')
    new_tiddler = store.get(new_tiddler)
    assert new_tiddler.title == 'binary'
    assert new_tiddler.type == 'application/binary'
    assert tiddler.text == b64encode('not really binary')

def test_handle_empty_policy():
    bag = Bag('empty')
    store.put(bag)
    new_bag = store.get(Bag('empty'))
    assert new_bag.policy.read == []
    assert new_bag.policy.manage == []
    assert new_bag.policy.create == []
    assert new_bag.policy.write == []
    assert new_bag.policy.accept == []
    assert new_bag.policy.owner == None

def test_tiddler_revisions():
    bag_name = u'bag8'
    for i in xrange(20):
        tiddler = Tiddler(u'oh hi', bag_name)
        tiddler.text = u'%s times we go' % i
        tiddler.fields[u'%s' % i] = u'%s' % i
        tiddler.fields[u'other%s' % i] = u'%s' % i
        tiddler.fields[u'carutther%s' % i] = u'x%s' % i
        store.put(tiddler)

    revisions = store.list_tiddler_revisions(Tiddler('oh hi', bag_name))
    assert len(revisions) == 20
    first_revision = revisions[-1]
    tiddler = Tiddler('oh hi', bag_name)
    tiddler.revision = first_revision + 13
    tiddler = store.get(tiddler)
    assert tiddler.title == 'oh hi'
    assert tiddler.text == '13 times we go'
    assert tiddler.fields['13'] == '13'
    assert tiddler.fields['other13'] == '13'
    assert tiddler.fields['carutther13'] == 'x13'
    assert '12' not in tiddler.fields

    tiddler.revision = 90
    py.test.raises(NoTiddlerError, 'store.get(tiddler)')

    py.test.raises(NoTiddlerError,
            'store.list_tiddler_revisions(Tiddler("sleepy", "cow"))')

def test_interleaved_tiddler_revisions():
    bag_name = u'bag8'
    for i in xrange(20):
        tiddler1 = Tiddler(u'oh yes', bag_name)
        tiddler2 = Tiddler(u'oh no', bag_name)
        tiddler1.text = u'%s times we yes' % i
        tiddler2.text = u'%s times we no' % i
        tiddler1.fields[u'%s' % i] = u'%s' % i
        tiddler2.fields[u'%s' % i] = u'%s' % i
        store.put(tiddler1)
        store.put(tiddler2)

    revisions = store.list_tiddler_revisions(Tiddler('oh yes', bag_name))
    assert len(revisions) == 20
    first_revision = revisions[-1]
    tiddler = Tiddler('oh yes', bag_name)
    tiddler.revision = first_revision + 26 
    tiddler = store.get(tiddler)
    assert tiddler.title == 'oh yes'
    assert tiddler.text == '13 times we yes'
    assert tiddler.fields['13'] == '13'
    assert '12' not in tiddler.fields

    tiddler.revision = 9999999 # big number to avoid auto increment issues
    py.test.raises(NoTiddlerError, 'store.get(tiddler)')

    py.test.raises(NoTiddlerError,
            'store.list_tiddler_revisions(Tiddler("sleepy", "cow"))')

def test_tiddler_no_bag():
    tiddler = Tiddler('hi')
    py.test.raises(NoBagError, 'store.put(tiddler)')

def test_list_tiddlers_no_bag():
    bag = Bag('carne')
    try:
        py.test.raises(NoBagError, 'store.list_bag_tiddlers(bag).next()')
    except AttributeError:
        assert True

def xtest_case_sensitive():
    bag = Bag('testcs')
    store.put(bag)

    tiddlera = Tiddler('testtiddler', 'testcs')
    tiddlera.text = u'a'
    store.put(tiddlera)
    tiddlerb = Tiddler('TestTiddler', 'testcs')
    tiddlerb.text = u'b'
    store.put(tiddlerb)

    tiddlerc = Tiddler('TestTiddler', 'testcs')
    tiddlerc = store.get(tiddlerc)
    assert tiddlerc.text == u'b'

    tiddlerd = Tiddler('testtiddler', 'testcs')
    tiddlerd = store.get(tiddlerd)
    assert tiddlerd.text == u'a'

def test_2bag_policy():
    bag = Bag(u'pone')
    bag.policy.read = [u'cdent']
    bag.policy.write = [u'cdent']
    store.put(bag)

    bag = Bag(u'ptwo')
    bag.policy.read = [u'cdent', u'fnd']
    bag.policy.write = [u'cdent']
    store.put(bag)

    pone = store.get(Bag(u'pone'))
    ptwo = store.get(Bag(u'ptwo'))

    assert pone.policy.read == [u'cdent']
    assert pone.policy.write == [u'cdent']

    assert sorted(ptwo.policy.read) == [u'cdent', u'fnd']
    assert ptwo.policy.write == [u'cdent']

    store.delete(pone)

    ptwo = store.get(Bag(u'ptwo'))

    assert sorted(ptwo.policy.read) == [u'cdent', u'fnd']
    assert ptwo.policy.write == [u'cdent']

    bag = Bag(u'pone')
    bag.policy.read = [u'cdent']
    bag.policy.write = [u'cdent']
    store.put(bag)

    pone = store.get(Bag(u'pone'))
    assert pone.policy.read == [u'cdent']
    assert pone.policy.write == [u'cdent']

    pone.policy.read.append(u'fnd')

    store.put(pone)

    pone = store.get(Bag(u'pone'))

    assert sorted(pone.policy.read) == [u'cdent', u'fnd']

def test_2recipe_policy():
    recipe = Recipe(u'pone')
    recipe.policy.read = [u'cdent']
    recipe.policy.write = [u'cdent']
    store.put(recipe)

    recipe = Recipe(u'ptwo')
    recipe.policy.read = [u'cdent', u'fnd']
    recipe.policy.write = [u'cdent']
    store.put(recipe)

    pone = store.get(Recipe(u'pone'))
    ptwo = store.get(Recipe(u'ptwo'))

    assert pone.policy.read == [u'cdent']
    assert pone.policy.write == [u'cdent']

    assert sorted(ptwo.policy.read) == [u'cdent', u'fnd']
    assert ptwo.policy.write == [u'cdent']

    store.delete(pone)

    ptwo = store.get(Recipe(u'ptwo'))

    assert sorted(ptwo.policy.read) == [u'cdent', u'fnd']
    assert ptwo.policy.write == [u'cdent']

    recipe = Recipe(u'pone')
    recipe.policy.read = [u'cdent']
    recipe.policy.write = [u'cdent']
    store.put(recipe)

    pone = store.get(Recipe(u'pone'))
    assert pone.policy.read == [u'cdent']
    assert pone.policy.write == [u'cdent']

    pone.policy.read.append(u'fnd')

    store.put(pone)

    pone = store.get(Recipe(u'pone'))

    assert sorted(pone.policy.read) == [u'cdent', u'fnd']

def test_revisions_deletions():
    tiddler = Tiddler(u'tone', u'pone')
    tiddler.text = u'revision1'
    tiddler.tags = [u'1', u'2']
    store.put(tiddler)
    tiddler.text = u'revision2'
    tiddler.tags = [u'3', u'4']
    store.put(tiddler)

    revisions = store.list_tiddler_revisions(tiddler)

    assert len(revisions) == 2

    store.delete(tiddler)

    py.test.raises(NoTiddlerError, 'store.list_tiddler_revisions(tiddler)')


def test_bag_deletes_tiddlers():
    tiddler = Tiddler(u'tone', u'pone')
    tiddler.text = u''
    store.put(tiddler)
    tiddler = Tiddler(u'uone', u'pone')
    tiddler.text = u''
    store.put(tiddler)

    bag = Bag(u'pone')

    tiddlers = list(store.list_bag_tiddlers(bag))
    assert len(tiddlers) == 2

    store.delete(bag)

    bag = Bag(u'pone')
    py.test.raises(NoBagError, 'list(store.list_bag_tiddlers(bag))')
    py.test.raises(NoTiddlerError, 'store.list_tiddler_revisions(tiddler)')

def test_multi_same_tag_tiddler():
    bag = Bag(u'holder')
    store.put(bag)
    tiddler = Tiddler('me', 'holder')
    tiddler.text = 'hi'
    tiddler.tags = ['foo']
    store.put(tiddler)

    tiddler2 = Tiddler('me', 'holder')
    tiddler2 = store.get(tiddler2)
    tiddler2.tags.append('bar')
    tiddler2.tags.append('bar')
    store.put(tiddler2)

    tiddler3 = store.get(Tiddler('me', 'holder'))
    assert sorted(tiddler3.tags) == ['bar', 'foo']

def test_multi_role_user():
    user = User(u'cdent')
    user.add_role('cow')
    user.add_role('cow')
    store.put(user)

    user2 = store.get(User(u'cdent'))
    assert list(user2.roles) == ['cow']
