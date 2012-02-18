from keystone.identity.backends.zk import *


def clear():
    import pykeeper
    client = pykeeper.ZooKeeper('localhost:2181')
    client.connect()
    if 'keystone' in client.get_children('/'):
        client.delete_recursive('/keystone')

clear()

i = Identity()

print 'users: %d' % len(i.list_users())
print 'tenants: %d' % len(i.list_tenants())
print 'roles: %d' % len(i.list_roles())

for user in i.list_users():
    i.delete_user(user['id'])

for tenant in i.list_tenants():
    i.delete_tenant(tenant['id'])

for role in i.list_roles():
    i.delete_role(role['id'])



# i.create_user('123', {'id': 123, 'name': 'joe'})
# print i.get_user('123')
# print i.get_user_by_name('joe')
# i.create_tenant('abc', {'id': 'abc', 'name': 'accounting'})
# i.add_user_to_tenant('abc', '123')

# print 'users', i.list_users()
# print 'tenants', i.list_tenants()
