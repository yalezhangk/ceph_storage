#!/usr/bin/env python
"""CLI interface for stor management."""

from __future__ import print_function

import logging as python_logging
import sys
import time

import prettytable
from oslo_config import cfg
from oslo_db import exception as db_exc
from oslo_db.sqlalchemy import migration
from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import context
from DSpace import db
from DSpace import exception
from DSpace import objects
# Need to register global_opts
from DSpace.common import config  # noqa
from DSpace.db import migration as db_migration
from DSpace.db.sqlalchemy import api as db_api
from DSpace.i18n import _
from DSpace.objects import fields as s_fields

try:
    import collections.abc as collections
except ImportError:
    import collections


CONF = cfg.CONF

LOG = logging.getLogger(__name__)


# Decorators for actions
def args(*args, **kwargs):
    def _decorator(func):
        func.__dict__.setdefault('args', []).insert(0, (args, kwargs))
        return func
    return _decorator


class ShellCommands(object):
    def bpython(self):
        """Runs a bpython shell.

        Falls back to Ipython/python shell if unavailable
        """
        self.run('bpython')

    def ipython(self):
        """Runs an Ipython shell.

        Falls back to Python shell if unavailable
        """
        self.run('ipython')

    def python(self):
        """Runs a python shell.

        Falls back to Python shell if unavailable
        """
        self.run('python')

    @args('--shell',
          metavar='<bpython|ipython|python>',
          help='Python shell')
    def run(self, shell=None):
        """Runs a Python interactive interpreter."""
        if not shell:
            shell = 'bpython'

        if shell == 'bpython':
            try:
                import bpython
                bpython.embed()
            except ImportError:
                shell = 'ipython'
        if shell == 'ipython':
            try:
                from IPython import embed
                embed()
            except ImportError:
                try:
                    # Ipython < 0.11
                    # Explicitly pass an empty list as arguments, because
                    # otherwise IPython would use sys.argv from this script.
                    import IPython

                    shell = IPython.Shell.IPShell(argv=[])
                    shell.mainloop()
                except ImportError:
                    # no IPython module
                    shell = 'python'

        if shell == 'python':
            import code
            try:
                # Try activating rlcompleter, because it's handy.
                import readline
            except ImportError:
                pass
            else:
                # We don't have to wrap the following import in a 'try',
                # because we already know 'readline' was imported successfully.
                import rlcompleter    # noqa
                readline.parse_and_bind("tab:complete")
            code.interact()

    @args('--path', required=True, help='Script path')
    def script(self, path):
        """Runs the script from the specified path with flags set properly."""
        exec(compile(open(path).read(), path, 'exec'), locals(), globals())


class DbCommands(object):
    """Class for managing the database."""

    # NOTE: Online migrations cannot depend on having Stor services running.
    # Migrations can be called during Fast-Forward Upgrades without having any
    # Stor services up.
    # NOTE; Online migrations must be removed at the beginning of the next
    # release to the one they've been introduced.  A comment with the release
    # a migration is introduced and the one where it must be removed must
    # preceed any element of the "online_migrations" tuple, like this:
    #    # Added in Queens remove in Rocky
    #    db.service_uuids_online_data_migration,
    online_migrations = tuple(
    )

    def __init__(self):
        pass

    @args('version', nargs='?', default=None, type=int,
          help='Database version')
    @args('--bump-versions', dest='bump_versions', default=False,
          action='store_true',
          help='Update RPC and Objects versions when doing offline upgrades, '
               'with this we no longer need to restart the services twice '
               'after the upgrade to prevent ServiceTooOld exceptions.')
    def sync(self, version=None, bump_versions=False):
        """Sync the database up to the most recent version."""
        if version is not None and version > db.MAX_INT:
            print(_('Version should be less than or equal to '
                    '%(max_version)d.') % {'max_version': db.MAX_INT})
            sys.exit(1)
        try:
            result = db_migration.db_sync(version)
        except db_exc.DBMigrationError as ex:
            print("Error during database migration: %s" % ex)
            sys.exit(1)

        return result

    @args('data', type=str, help='Init data')
    def sys_config(self, data):
        configs = data.split('|')
        ctxt = context.get_context()
        allowed = {
            'image_name': s_fields.ConfigType.STRING,
            'image_namespace': s_fields.ConfigType.STRING,
            'dspace_version': s_fields.ConfigType.STRING,
            'admin_ip_address': s_fields.ConfigType.STRING,
            'agent_port': s_fields.ConfigType.INT,
            'admin_port': s_fields.ConfigType.INT,
            'dspace_repo': s_fields.ConfigType.STRING,
            'config_dir': s_fields.ConfigType.STRING,
            'run_dir': s_fields.ConfigType.STRING,
            'config_dir_container': s_fields.ConfigType.STRING,
            'log_dir': s_fields.ConfigType.STRING,
            'log_dir_container': s_fields.ConfigType.STRING,
            'admin_ips': s_fields.ConfigType.STRING,
            'max_osd_num': s_fields.ConfigType.INT,
            'max_monitor_num': s_fields.ConfigType.INT,
            'dspace_dir': s_fields.ConfigType.STRING,
            'node_exporter_port': s_fields.ConfigType.INT,
            'debug_mode': s_fields.ConfigType.BOOL,
            'ceph_monitor_port': s_fields.ConfigType.INT,
            'mgr_dspace_port': s_fields.ConfigType.INT,
            'dsa_socket_file': s_fields.ConfigType.STRING
        }
        for c in configs:
            key, value = c.split("=", 1)
            if key not in allowed:
                raise exception.InvalidInput("key %s not support" % key)
            sys_configs = objects.SysConfigList.get_all(
                ctxt, filters={'key': key})
            value_type = allowed.get(key)
            if not sys_configs:
                objects.SysConfig(
                    ctxt, key=key, value=value,
                    value_type=value_type,
                ).create()
            else:
                sys_config = sys_configs[0]
                sys_config.value = value
                sys_config.save()

    @args('data', type=str, help='Init data')
    def rpc_service(self, data):
        configs = data.split('|')
        ctxt = context.get_context()
        rpc_service = objects.RPCService(
            ctxt
        )
        endpint = {}
        allowed = ["ip", "port", "hostname", "service_name"]
        for c in configs:
            key, value = c.split("=", 1)
            if key not in allowed:
                raise exception.InvalidInput("key %s not support" % key)
            if key == 'ip':
                endpint['ip'] = value
            if key == 'port':
                endpint['port'] = value
            if key == 'hostname':
                rpc_service.hostname = value
            if key == 'service_name':
                rpc_service.service_name = value
        rpc_service.endpoint = endpint
        rpc_service.create()

    def version(self):
        """Print the current database version."""
        print(migration.db_version(db_api.get_engine(),
                                   db_migration.MIGRATE_REPO_PATH,
                                   db_migration.INIT_VERSION))

    @args('age_in_days', type=int,
          help='Purge deleted rows older than age in days')
    def purge(self, age_in_days):
        """Purge deleted rows older than a given age from DSpace tables."""
        age_in_days = int(age_in_days)
        if age_in_days < 0:
            print(_("Must supply a positive value for age"))
            sys.exit(1)
        if age_in_days >= (int(time.time()) / 86400):
            print(_("Maximum age is count of days since epoch."))
            sys.exit(1)
        ctxt = context.get_admin_context()

        try:
            db.purge_deleted_rows(ctxt, age_in_days)
        except db_exc.DBReferenceError:
            print(_("Purge command failed, check stor-manage "
                    "logs for more details."))
            sys.exit(1)

    def _run_migration(self, ctxt, max_count):
        ran = 0
        exceptions = False
        migrations = {}
        for migration_meth in self.online_migrations:
            count = max_count - ran
            try:
                found, done = migration_meth(ctxt, count)
            except Exception:
                msg = (_("Error attempting to run %(method)s") %
                       {'method': migration_meth.__name__})
                print(msg)
                LOG.exception(msg)
                exceptions = True
                found = done = 0

            name = migration_meth.__name__
            if found:
                print(_('%(found)i rows matched query %(meth)s, %(done)i '
                        'migrated') % {'found': found,
                                       'meth': name,
                                       'done': done})
            migrations[name] = found, done
            if max_count is not None:
                ran += done
                if ran >= max_count:
                    break
        return migrations, exceptions

    @args('--max_count', metavar='<number>', dest='max_count', type=int,
          help='Maximum number of objects to consider.')
    def online_data_migrations(self, max_count=None):
        """Perform online data migrations for the release in batches."""
        ctxt = context.get_admin_context()
        if max_count is not None:
            unlimited = False
            if max_count < 1:
                print(_('Must supply a positive value for max_count.'))
                sys.exit(127)
        else:
            unlimited = True
            max_count = 50
            print(_('Running batches of %i until complete.') % max_count)

        ran = None
        exceptions = False
        migration_info = {}
        while ran is None or ran != 0:
            migrations, exceptions = self._run_migration(ctxt, max_count)
            ran = 0
            for name in migrations:
                migration_info.setdefault(name, (0, 0))
                migration_info[name] = (
                    max(migration_info[name][0], migrations[name][0]),
                    migration_info[name][1] + migrations[name][1],
                )
                ran += migrations[name][1]
            if not unlimited:
                break
        headers = ["{}".format(_('Migration')),
                   "{}".format(_('Total Needed')),
                   "{}".format(_('Completed')), ]
        t = prettytable.PrettyTable(headers)
        for name in sorted(migration_info.keys()):
            info = migration_info[name]
            t.add_row([name, info[0], info[1]])
        print(t)

        # NOTE(imacdonn): In the "unlimited" case, the loop above will only
        # terminate when all possible migrations have been effected. If we're
        # still getting exceptions, there's a problem that requires
        # intervention. In the max-count case, exceptions are only considered
        # fatal if no work was done by any other migrations ("not ran"),
        # because otherwise work may still remain to be done, and that work
        # may resolve dependencies for the failing migrations.
        if exceptions and (unlimited or not ran):
            print(_("Some migrations failed unexpectedly. Check log for "
                    "details."))
            sys.exit(2)

        sys.exit(1 if ran else 0)

    @args('--enable-replication', action='store_true', default=False,
          help='Set replication status to enabled (default: %(default)s).')
    @args('--active-backend-id', default=None,
          help='Change the active backend ID (default: %(default)s).')
    @args('--backend-host', required=True,
          help='The backend host name.')
    def reset_active_backend(self, enable_replication, active_backend_id,
                             backend_host):
        """Reset the active backend for a host."""

        ctxt = context.get_admin_context()

        try:
            db.reset_active_backend(ctxt, enable_replication,
                                    active_backend_id, backend_host)
        except db_exc.DBReferenceError:
            print(_("Failed to reset active backend for host %s, "
                    "check stor-manage logs for more details.") %
                  backend_host)
            sys.exit(1)


class ConfigCommands(object):
    """Class for exposing the flags defined by flag_file(s)."""

    def __init__(self):
        pass

    @args('param', nargs='?', default=None,
          help='Configuration parameter to display (default: %(default)s)')
    def list(self, param=None):
        """List parameters configured for stor.

        Lists all parameters configured for stor unless an optional argument
        is specified.  If the parameter is specified we only print the
        requested parameter.  If the parameter is not found an appropriate
        error is produced by .get*().
        """
        param = param and param.strip()
        if param:
            print('%s = %s' % (param, CONF.get(param)))
        else:
            for key, value in CONF.items():
                print('%s = %s' % (key, value))


class BaseCommand(object):
    @staticmethod
    def _normalize_time(time_field):
        return time_field and timeutils.normalize_time(time_field)

    @staticmethod
    def _state_repr(is_up):
        return ':-)' if is_up else 'XXX'


CATEGORIES = {
    'config': ConfigCommands,
    'db': DbCommands,
    'shell': ShellCommands,
}


def methods_of(obj):
    """Return non-private methods from an object.

    Get all callable methods of an object that don't start with underscore
    :return: a list of tuples of the form (method_name, method)
    """
    result = []
    for i in dir(obj):
        if isinstance(getattr(obj, i),
                      collections.Callable) and not i.startswith('_'):
            result.append((i, getattr(obj, i)))
    return result


def add_command_parsers(subparsers):
    for category in sorted(CATEGORIES):
        command_object = CATEGORIES[category]()

        parser = subparsers.add_parser(category)
        parser.set_defaults(command_object=command_object)

        category_subparsers = parser.add_subparsers(dest='action')

        for (action, action_fn) in methods_of(command_object):
            parser = category_subparsers.add_parser(action)

            action_kwargs = []
            for args, kwargs in getattr(action_fn, 'args', []):
                parser.add_argument(*args, **kwargs)

            parser.set_defaults(action_fn=action_fn)
            parser.set_defaults(action_kwargs=action_kwargs)


category_opt = cfg.SubCommandOpt('category',
                                 title='Command categories',
                                 handler=add_command_parsers)


def get_arg_string(args):
    if args[0] == '-':
        # (Note)zhiteng: args starts with FLAGS.oparser.prefix_chars
        # is optional args. Notice that cfg module takes care of
        # actual ArgParser so prefix_chars is always '-'.
        if args[1] == '-':
            # This is long optional arg
            args = args[2:]
        else:
            args = args[1:]

    # We convert dashes to underscores so we can have cleaner optional arg
    # names
    if args:
        args = args.replace('-', '_')

    return args


def fetch_func_args(func):
    fn_kwargs = {}
    for args, kwargs in getattr(func, 'args', []):
        # Argparser `dest` configuration option takes precedence for the name
        arg = kwargs.get('dest') or get_arg_string(args[0])
        fn_kwargs[arg] = getattr(CONF.category, arg)

    return fn_kwargs


def main():
    objects.register_all()
    """Parse options and call the appropriate class/method."""
    CONF.register_cli_opt(category_opt)
    script_name = sys.argv[0]
    if len(sys.argv) < 2:
        print(script_name + " category action [<args>]")
        print(_("Available categories:"))
        for category in CATEGORIES:
            print(_("\t%s") % category)
        sys.exit(2)

    try:
        CONF(sys.argv[1:], project='stor')
        logging.setup(CONF, "stor")
        python_logging.captureWarnings(True)
    except cfg.ConfigDirNotFoundError as details:
        print(_("Invalid directory: %s") % details)
        sys.exit(2)
    except cfg.ConfigFilesNotFoundError as e:
        cfg_files = e.config_files
        print(_("Failed to read configuration file(s): %s") % cfg_files)
        sys.exit(2)

    fn = CONF.category.action_fn
    fn_kwargs = fetch_func_args(fn)
    fn(**fn_kwargs)


if __name__ == '__main__':
    main()
