# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 OpenStack LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from keystone import identity
from keystone.common import kvs
from keystone.common import utils

import pykeeper
import json


def _filter_user(user_ref):
    if user_ref:
        user_ref = user_ref.copy()
        user_ref.pop('password', None)
        user_ref.pop('tenants', None)
    return user_ref


def _ensure_hashed_password(user_ref):
    pw = user_ref.get('password', None)
    if pw is not None:
        user_ref['password'] = utils.hash_password(pw)
    return user_ref


class Identity(kvs.Base, identity.Driver):

    def __init__(self, client=None):
        self._zk_client = client

    @property
    def client(self):
        return self._zk_client or self._get_zk_client()

    def _get_zk_client(self):
        self._zk_client = pykeeper.ZooKeeper('localhost:2181')
        self._zk_client.connect()
        return self._zk_client

    def _path(self, parts):
        return '/keystone/' + '/'.join([str(p) for p in parts])

    def _get(self, *parts):
        try:
            result = self.client.get(self._path(parts))
            return json.loads(result[0])
        except:
            pass

    def _list(self, *parts):
        try:
            return self.client.get_children(self._path(parts))
        except:
            return []

    def _delete(self, *parts):
        self.client.delete_recursive(self._path(parts))

    def _create(self, *parts):
        parts = list(parts)
        data = json.dumps(parts.pop())
        self.client.create_recursive(self._path(parts), data)

    def _set(self, *parts):
        parts = list(parts)
        data = json.dumps(parts.pop())
        self.client.set(self._path(parts), data)

    def authenticate(self, user_id=None, tenant_id=None, password=None):
        """Authenticate based on a user, tenant and password.

        Expects the user object to have a password field and the tenant to be
        in the list of tenants on the user.

        """
        user_ref = self._get_user(user_id)
        tenant_ref = None
        metadata_ref = None
        if (not user_ref
            or not utils.check_password(password, user_ref.get('password'))):
            raise AssertionError('Invalid user / password')
        if tenant_id and tenant_id not in user_ref['tenants']:
            raise AssertionError('Invalid tenant')

        tenant_ref = self.get_tenant(tenant_id)
        if tenant_ref:
            metadata_ref = self.get_metadata(user_id, tenant_id)
        else:
            metadata_ref = {}
        return (_filter_user(user_ref), tenant_ref, metadata_ref)

    def get_tenant(self, tenant_id):
        return self._get('tenant', tenant_id)

    def get_tenant_by_name(self, tenant_name):
        for row in self._list('tenant'):
            tenant_ref = self.get_tenant(row)
            if tenant_ref['name'] == tenant_name:
                return tenant_ref

    def _get_user(self, user_id):
        user_ref = self._get('user', user_id)
        if user_ref:
            user_ref['tenants'] = self.get_tenants_for_user(user_id)
        return user_ref

    def _get_user_by_name(self, user_name):
        for row in self._list('user'):
            user_ref = self._get_user(row)
            if user_ref['name'] == user_name:
                return user_ref

    def get_user(self, user_id):
        return _filter_user(self._get_user(user_id))

    def get_user_by_name(self, user_name):
        return _filter_user(self._get_user_by_name(user_name))

    def get_metadata(self, user_id, tenant_id):
        return self._get('tenant', tenant_id, 'user', user_id, 'metadata')

    def get_role(self, role_id):
        return self._get('role', role_id)

    def list_tenants(self):
        return [self.get_tenant(x) for x in self._list('tenant')]

    def list_users(self):
        return [self.get_user(x) for x in self._list('user')]

    def list_roles(self):
        return [self.get_role(x) for x in self._list('role')]

    # These should probably be part of the high-level API
    def add_user_to_tenant(self, tenant_id, user_id):
        self._create('user', user_id, 'tenant', tenant_id, None)
        self._create('tenant', tenant_id, 'user', user_id, None)

    def remove_user_from_tenant(self, tenant_id, user_id):
        self._delete('user', user_id, 'tenant', tenant_id)
        self._delete('tenant', tenant_id, 'user', user_id)

    def get_tenants_for_user(self, user_id):
        return self._list('user', user_id, 'tenant')

    def get_roles_for_user_and_tenant(self, user_id, tenant_id):
        return self._list('tenant', tenant_id, 'user', user_id, 'role')

    def add_role_to_user_and_tenant(self, user_id, tenant_id, role_id):
        self._create('tenant', tenant_id, 'user', user_id, 'role', role_id, None)

    def remove_role_from_user_and_tenant(self, user_id, tenant_id, role_id):
        self._delete('tenant', tenant_id, 'user', user_id, 'role', role_id)

    # CRUD
    def create_user(self, user_id, user):
        if self.get_user(user_id):
            raise Exception('Duplicate id')
        if self.get_user_by_name(user['name']):
            raise Exception('Duplicate name')
        user = _ensure_hashed_password(user)
        self._create('user', user_id, user)
        return user

    def update_user(self, user_id, user):
        if 'name' in user:
            existing = self.get_user_by_name(user['name'])
            if existing and user_id != existing['id']:
                raise Exception('Duplicate name')
        # get the old name and delete it too
        old_user = self.get_user(user_id)
        new_user = old_user.copy()
        user = _ensure_hashed_password(user)
        new_user.update(user)
        new_user['id'] = user_id
        self._set('user', user_id, new_user)
        return new_user

    def delete_user(self, user_id):
        self._delete('user', user_id)

    def create_tenant(self, tenant_id, tenant):
        if self.get_tenant(tenant_id):
            raise Exception('Duplicate id')
        if self.get_tenant_by_name(tenant['name']):
            raise Exception('Duplicate name')
        self._create('tenant', tenant_id, tenant)
        return tenant

    def update_tenant(self, tenant_id, tenant):
        if 'name' in tenant:
            existing = self.get_tenant_by_name(tenant['name'])
            if existing and tenant_id != existing['id']:
                raise Exception('Duplicate name')
        # get the old name and delete it too
        old_tenant = self.get_tenant(tenant_id)
        new_tenant = old_tenant.copy()
        new_tenant['id'] = tenant_id
        self._set('tenant', tenant_id, new_tenant)
        return tenant

    def delete_tenant(self, tenant_id):
        self._delete('tenant', tenant_id)

    def create_metadata(self, user_id, tenant_id, metadata):
        self._create('tenant', tenant_id, 'user', user_id, 'metadata', metadata)
        return metadata

    def update_metadata(self, user_id, tenant_id, metadata):
        roles = metadata.get('roles', [])
        # we seem to be storing roles in metadata as well as as a first class
        for role_id in roles:
            self.add_role_to_user_and_tenant(user_id, tenant_id, role_id)
        for role_id in self.get_roles_for_user_and_tenant(user_id, tenant_id):
            if not role_id in roles:
                self.remove_role_from_user_and_tenant(user_id, tenant_id, role_id)

        self._set('tenant', tenant_id, 'user', user_id, 'metadata', metadata)
        return metadata

    def delete_metadata(self, user_id, tenant_id):
        self._delete('tenant', tenant_id, 'user', user_id, 'metadata')

    def create_role(self, role_id, role):
        self._create('role', role_id, role)
        return role

    def update_role(self, role_id, role):
        self._set('role', role_id, role)
        return role

    def delete_role(self, role_id):
        self._delete('role', role_id)
        return None
