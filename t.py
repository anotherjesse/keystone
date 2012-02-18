from keystone.identity.backends.zk import *


def clear():
    import pykeeper
    client = pykeeper.ZooKeeper('localhost:2181')
    client.connect()
    if 'keystone' in client.get_children('/'):
        client.delete_recursive('/keystone')


i = Identity()

def status():
	print 'users: %d' % len(i.list_users())
	print 'tenants: %d' % len(i.list_tenants())
	print 'roles: %d' % len(i.list_roles())

status()

clear()

i.create_role('admin', {'id': 'admin', 'name': 'admin'})

i.create_user('123', {'id': 123, 'name': 'joe'})
print i.get_user('123')
print i.get_user_by_name('joe')

i.create_tenant('abc', {'id': 'abc', 'name': 'accounting'})
i.add_user_to_tenant('abc', '123')

print 'tenants for 123: ', i.get_tenants_for_user('123')

i.add_role_to_user_and_tenant('123', 'abc', 'admin')
print 'roles for 123 in abc', i.get_roles_for_user_and_tenant('123', 'abc')

status()
