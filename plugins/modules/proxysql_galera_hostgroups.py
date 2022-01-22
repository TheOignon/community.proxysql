#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright: (c) 2017, Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type


from ansible.module_utils.basic import AnsibleModule
from ansible_collections.community.proxysql.plugins.module_utils.mysql import (
    mysql_connect,
    mysql_driver,
    proxysql_common_argument_spec,
    save_config_to_disk,
    load_config_to_runtime,
)
from ansible.module_utils._text import to_native

# ===========================================
# proxysql module specific support methods.
#


def perform_checks(module):
    if not module.params["writer_hostgroup"] >= 0:
        module.fail_json(
            msg="writer_hostgroup must be an integer greater than or equal to 0"
        )

    if not module.params["backup_writer_hostgroup"] >= 0:
        module.fail_json(
            msg="backup_writer_hostgroup must be an integer greater than or equal to 0"
        )

    if module.params["reader_hostgroup"] < 0:
        module.fail_json(
            msg="reader_hostgroup must be an integer greater than or equal to 0"
        )

    if module.params["offline_hostgroup"] < 0:
        module.fail_json(
            msg="offline_hostgroup must be an integer greater than or equal to 0"
        )

    if module.params["backup_writer_hostgroup"] == module.params["writer_hostgroup"]:
        module.fail_json(
            msg="backup_writer_hostgroup and writer_hostgroup must be different integer values"
        )

    if module.params["reader_hostgroup"] == module.params["writer_hostgroup"]:
        module.fail_json(
            msg="reader_hostgroup and writer_hostgroup must be different integer values"
        )

    if module.params["backup_writer_hostgroup"] == module.params["reader_hostgroup"]:
        module.fail_json(
            msg="backup_writer_hostgroup and reader_hostgroup must be different integer values"
        )

    if module.params["offline_hostgroup"] == module.params["writer_hostgroup"]:
        module.fail_json(
            msg="offline_hostgroup and writer_hostgroup must be different integer values"
        )

    if module.params["offline_hostgroup"] == module.params["reader_hostgroup"]:
        module.fail_json(
            msg="offline_hostgroup and reader_hostgroup must be different integer values"
        )

    if module.params["backup_writer_hostgroup"] == module.params["offline_hostgroup"]:
        module.fail_json(
            msg="backup_writer_hostgroup and offline_hostgroup must be different integer values"
        )

    if not module.params["active"] in [0,1]:
        module.fail_json(
            msg="active must be either 0 or 1"
        )

    if module.params["max_writers"] < 0:
        module.fail_json(
            msg="max_writers must be an integer greater than or equal to 0"
        )

    if not module.params["writer_is_also_reader"] in [0, 1, 2]:
        module.fail_json(
            msg="writer_is_also_reader must be an integer in 0, 1 or 2"
        )

    if module.params["max_transactions_behind"] < 0:
        module.fail_json(
            msg="max_transactions_behind must be an integer greater than or equal to 0"
        )

class ProxySQLGaleraHostgroup(object):

    def __init__(self, module, version):
        self.state = module.params["state"]
        self.save_to_disk = module.params["save_to_disk"]
        self.load_to_runtime = module.params["load_to_runtime"]
        self.writer_hostgroup = module.params["writer_hostgroup"]
        self.backup_writer_hostgroup = module.params["backup_writer_hostgroup"]
        self.reader_hostgroup = module.params["reader_hostgroup"]
        self.offline_hostgroup = module.params["offline_hostgroup"]
        self.active = module.params["active"]
        self.max_writers = module.params["max_writers"]
        self.writer_is_also_reader = module.params["writer_is_also_reader"]
        self.max_transactions_behind = module.params["max_transactions_behind"]
        self.comment = module.params["comment"]
        self.check_mode = module.check_mode

    def check_galera_group_config(self, cursor, keys):
        query_string = \
            """SELECT count(*) AS `galera_groups`
               FROM mysql_galera_hostgroups
               WHERE writer_hostgroup = %s"""

        query_data = \
            [self.writer_hostgroup]

        cursor.execute(query_string, query_data)
        check_count = cursor.fetchone()
        return (int(check_count['galera_groups']) > 0)

    def get_galera_group_config(self, cursor):
        query_string = \
            """SELECT *
               FROM mysql_galera_hostgroups
               WHERE writer_hostgroup = %s"""

        query_data = \
            [self.writer_hostgroup]

        cursor.execute(query_string, query_data)
        galera_group = cursor.fetchone()
        return galera_group

    def create_galera_group_config(self, cursor):
        query_string = \
            """INSERT INTO mysql_galera_hostgroups (
               writer_hostgroup,
               backup_writer_hostgroup,
               reader_hostgroup,
               offline_hostgroup,
               active,
               max_writers,
               writer_is_also_reader,
               max_transactions_behind,
               comment)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        query_data = \
            [self.writer_hostgroup,
             self.backup_writer_hostgroup,
             self.reader_hostgroup,
             self.offline_hostgroup,
             self.active,
             self.max_writers,
             self.writer_is_also_reader,
             self.max_transactions_behind,
             self.comment or '']

        cursor.execute(query_string, query_data)

        if self.check_type_support:
            self.update_check_type(cursor)

        return True

    def delete_galera_group_config(self, cursor):
        query_string = \
            """DELETE FROM mysql_galera_hostgroups
               WHERE writer_hostgroup = %s"""

        query_data = \
            [self.writer_hostgroup]

        cursor.execute(query_string, query_data)
        return True

    def manage_config(self, cursor, state):
        if state and not self.check_mode:
            if self.save_to_disk:
                save_config_to_disk(cursor, "SERVERS")
            if self.load_to_runtime:
                load_config_to_runtime(cursor, "SERVERS")

    def create_galera_group(self, result, cursor):
        if not self.check_mode:
            result['changed'] = \
                self.create_galera_group_config(cursor)
            result['msg'] = "Added server to mysql_hosts"
            result['galera_group'] = \
                self.get_galera_group_config(cursor)
            self.manage_config(cursor,
                               result['changed'])
        else:
            result['changed'] = True
            result['msg'] = ("Galera group would have been added to" +
                             " mysql_galera_hostgroups, however" +
                             " check_mode is enabled.")

    def update_galera_group(self, result, cursor):
        current = self.get_galera_group_config(cursor)

        if current.get('comment') != self.comment:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_comment(cursor)

        if int(current.get('writer_hostgroup')) != self.writer_hostgroup:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_writer_hostgroup(cursor)

        if int(current.get('backup_writer_hostgroup')) != self.backup_writer_hostgroup:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_backup_writer_hostgroup(cursor)

        if int(current.get('reader_hostgroup')) != self.reader_hostgroup:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_reader_hostgroup(cursor)

        if int(current.get('offline_hostgroup')) != self.offline_hostgroup:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_offline_hostgroup(cursor)

        if int(current.get('active')) != self.active:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_active(cursor)

        if int(current.get('max_writers')) != self.max_writers:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_max_writers(cursor)

        if int(current.get('writer_is_also_reader')) != self.writer_is_also_reader:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_writer_is_also_reader(cursor)

        if int(current.get('max_transactions_behind')) != self.max_transactions_behind:
            result['changed'] = True
            result['msg'] = "Updated galera hostgroups in check_mode"
            if not self.check_mode:
                result['msg'] = "Updated galera hostgroups"
                self.update_max_transactions_behind(cursor)

        result['galera_group'] = self.get_galera_group_config(cursor)

        self.manage_config(cursor,
                           result['changed'])

    def delete_galera_group(self, result, cursor):
        if not self.check_mode:
            result['galera_group'] = \
                self.get_galera_group_config(cursor)
            result['changed'] = \
                self.delete_galera_group_config(cursor)
            result['msg'] = "Deleted server from mysql_hosts"
            self.manage_config(cursor,
                               result['changed'])
        else:
            result['changed'] = True
            result['msg'] = ("Repl group would have been deleted from" +
                             " mysql_galera_hostgroups, however" +
                             " check_mode is enabled.")

    def update_check_type(self, cursor):
        try:
            query_string = ("UPDATE mysql_galera_hostgroups "
                            "SET check_type = %s "
                            "WHERE writer_hostgroup = %s")

            cursor.execute(query_string, (self.check_type, self.writer_hostgroup))
        except Exception as e:
            pass

    def update_backup_writer_hostgroup(self, cursor):
        query_string = ("UPDATE mysql_galera_hostgroups "
                        "SET backup_writer_hostgroup = %s "
                        "WHERE writer_hostgroup = %s")

        cursor.execute(query_string, (self.backup_writer_hostgroup, self.writer_hostgroup))


    def update_reader_hostgroup(self, cursor):
        query_string = ("UPDATE mysql_galera_hostgroups "
                        "SET reader_hostgroup = %s "
                        "WHERE writer_hostgroup = %s")

        cursor.execute(query_string, (self.reader_hostgroup, self.writer_hostgroup))

    def update_offline_hostgroup(self, cursor):
        query_string = ("UPDATE mysql_galera_hostgroups "
                        "SET offline_hostgroup = %s "
                        "WHERE writer_hostgroup = %s")

        cursor.execute(query_string, (self.offline_hostgroup, self.writer_hostgroup))

    def update_active(self, cursor):
        query_string = ("UPDATE mysql_galera_hostgroups "
                        "SET active = %s "
                        "WHERE writer_hostgroup = %s")

        cursor.execute(query_string, (self.active, self.writer_hostgroup))

    def update_max_writers(self, cursor):
        query_string = ("UPDATE mysql_galera_hostgroups "
                        "SET max_writers = %s "
                        "WHERE writer_hostgroup = %s")

        cursor.execute(query_string, (self.max_writers, self.writer_hostgroup))

    def update_writer_is_also_reader(self, cursor):
        query_string = ("UPDATE mysql_galera_hostgroups "
                        "SET writer_is_also_reader = %s "
                        "WHERE writer_hostgroup = %s")

        cursor.execute(query_string, (self.writer_is_also_reader, self.writer_hostgroup))

    def update_max_transactions_behind(self, cursor):
        query_string = ("UPDATE mysql_galera_hostgroups "
                        "SET max_transactions_behind = %s "
                        "WHERE writer_hostgroup = %s")

        cursor.execute(query_string, (self.max_transactions_behind, self.writer_hostgroup))

    def update_comment(self, cursor):
        query_string = ("UPDATE mysql_galera_hostgroups "
                        "SET comment = %s "
                        "WHERE writer_hostgroup = %s ")

        cursor.execute(query_string, (self.comment, self.writer_hostgroup))


# ===========================================
# Module execution.
#
def main():
    argument_spec = proxysql_common_argument_spec()
    argument_spec.update(
        writer_hostgroup=dict(required=True, type='int'),
        backup_writer_hostgroup=dict(required=True, type='int'),
        reader_hostgroup=dict(required=True, type='int'),
        offline_hostgroup=dict(required=True, type='int'),
        active=dict(default=True, type='bool'),
        max_writers=dict(default=1, type='int'),
        writer_is_also_reader=dict(default=0, type='int'),
        max_transactions_behind=dict(default=0, type='int'),
        comment=dict(type='str', default=''),
        state=dict(default='present', choices=['present',
                                               'absent']),
        save_to_disk=dict(default=True, type='bool'),
        load_to_runtime=dict(default=True, type='bool')
    )

    module = AnsibleModule(
        supports_check_mode=True,
        argument_spec=argument_spec
    )

    perform_checks(module)

    login_user = module.params["login_user"]
    login_password = module.params["login_password"]
    config_file = module.params["config_file"]

    cursor = None
    try:
        cursor, db_conn, version = mysql_connect(module,
                                                 login_user,
                                                 login_password,
                                                 config_file,
                                                 cursor_class='DictCursor')
    except mysql_driver.Error as e:
        module.fail_json(
            msg="unable to connect to ProxySQL Admin Module.. %s" % to_native(e)
        )

    proxysql_galera_group = ProxySQLGaleraHostgroup(module, version)
    result = {}

    result['state'] = proxysql_galera_group.state
    result['changed'] = False

    if proxysql_galera_group.state == "present":
        try:
            if not proxysql_galera_group.check_galera_group_config(cursor,
                                                               keys=True):
                proxysql_galera_group.create_galera_group(result,
                                                      cursor)
            else:
                proxysql_galera_group.update_galera_group(result, cursor)

                result['galera_group'] = proxysql_galera_group.get_galera_group_config(cursor)

        except mysql_driver.Error as e:
            module.fail_json(
                msg="unable to modify galera hostgroup.. %s" % to_native(e)
            )

    elif proxysql_galera_group.state == "absent":
        try:
            if proxysql_galera_group.check_galera_group_config(cursor,
                                                           keys=True):
                proxysql_galera_group.delete_galera_group(result, cursor)
            else:
                result['changed'] = False
                result['msg'] = ("The repl group is already absent from the" +
                                 " mysql_galera_hostgroups memory" +
                                 " configuration")

        except mysql_driver.Error as e:
            module.fail_json(
                msg="unable to delete galera hostgroup.. %s" % to_native(e)
            )

    module.exit_json(**result)


if __name__ == '__main__':
    main()
