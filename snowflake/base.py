"""
Snowflake database backend for Django.

Requires snowflake connector for python
"""

import asyncio
import threading
import warnings
from contextlib import contextmanager

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError as WrappedDatabaseError, connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.utils import (
    CursorDebugWrapper as BaseCursorDebugWrapper,
)
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property
from django.utils.safestring import SafeString
from django.utils.version import get_version_tuple

try:
    import snowflake.connector as Database
except ImportError as e:
    raise ImproperlyConfigured("Error loading snowflake connector module: %s" % e)


# TODO: add versioning checks
# PSYCOPG2_VERSION = psycopg2_version()
#
# if PSYCOPG2_VERSION < (2, 5, 4):
#     raise ImproperlyConfigured("psycopg2_version 2.5.4 or newer is required; you have %s" % psycopg2.__version__)


# Some of these import snowflake connector, so import them after checking if it's installed.
from .client import DatabaseClient                          # NOQA isort:skip
from .creation import DatabaseCreation                      # NOQA isort:skip
from .features import DatabaseFeatures                      # NOQA isort:skip
from .introspection import DatabaseIntrospection            # NOQA isort:skip
from .operations import DatabaseOperations                  # NOQA isort:skip
from .schema import DatabaseSchemaEditor                    # NOQA isort:skip


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'snowflake'
    display_name = 'Snowflake'
    data_types = {
        'AutoField': 'serial',
        'BigAutoField': 'bigserial',
        'BinaryField': 'bytea',
        'BooleanField': 'boolean',
        'CharField': 'varchar(%(max_length)s)',
        'DateField': 'date',
        'DateTimeField': 'timestamp with time zone',
        'DecimalField': 'numeric(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'interval',
        'FileField': 'varchar(%(max_length)s)',
        'FilePathField': 'varchar(%(max_length)s)',
        'FloatField': 'double precision',
        'IntegerField': 'integer',
        'BigIntegerField': 'bigint',
        'IPAddressField': 'inet',
        'GenericIPAddressField': 'inet',
        'JSONField': 'jsonb',
        'NullBooleanField': 'boolean',
        'OneToOneField': 'integer',
        'PositiveBigIntegerField': 'bigint',
        'PositiveIntegerField': 'integer',
        'PositiveSmallIntegerField': 'smallint',
        'SlugField': 'varchar(%(max_length)s)',
        'SmallAutoField': 'smallserial',
        'SmallIntegerField': 'smallint',
        'TextField': 'text',
        'TimeField': 'time',
        'UUIDField': 'uuid',
    }
    data_type_check_constraints = {
        'PositiveBigIntegerField': '"%(column)s" >= 0',
        'PositiveIntegerField': '"%(column)s" >= 0',
        'PositiveSmallIntegerField': '"%(column)s" >= 0',
    }
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, E'\\', E'\\\\'), E'%%', E'\\%%'), E'_', E'\\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%'",
        'icontains': "LIKE '%%' || UPPER({}) || '%%'",
        'startswith': "LIKE {} || '%%'",
        'istartswith': "LIKE UPPER({}) || '%%'",
        'endswith': "LIKE '%%' || {}",
        'iendswith': "LIKE '%%' || UPPER({})",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor

    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def get_connection_params(self):
        settings_dict = self.settings_dict
        conn_params = {}

        if settings_dict['DATABASE']:
            conn_params['database'] = settings_dict['DATABASE']
        else:
            raise ImproperlyConfigured("Please provide a username for snowflake!")

        if settings_dict['USER']:
            conn_params['user'] = settings_dict['USER']
        else:
            raise ImproperlyConfigured("Please provide a username for snowflake!")

        if settings_dict['PASSWORD']:
            conn_params['password'] = settings_dict['PASSWORD']
        else:
            raise ImproperlyConfigured("Please provide a password for snowflake!")

        if settings_dict['ACCOUNT']:
            conn_params['account'] = settings_dict['ACCOUNT']
        else:
            raise ImproperlyConfigured("Please provide an account for snowflake!")

        if settings_dict['WAREHOUSE']:
            conn_params['warehouse'] = settings_dict['WAREHOUSE']
        else:
            raise ImproperlyConfigured("Please provide a warehouse for snowflake!")

        if settings_dict['ROLE']:
            conn_params['role'] = settings_dict['ROLE']
        else:
            raise ImproperlyConfigured("Please provide a role for snowflake!")

        if settings_dict['SCHEMA']:
            conn_params['schema'] = settings_dict['SCHEMA']
        else:
            raise ImproperlyConfigured("Please provide a schema for snowflake!")

        return conn_params

    @async_unsafe
    def get_new_connection(self, conn_params):
        connection = Database.connect(**conn_params)
        return connection

    def init_connection_state(self):
        pass

    @async_unsafe
    def create_cursor(self, name=None):
        cursor = self.connection.cursor()
        return cursor

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def is_usable(self):
        try:
            # Use a cursor directly, bypassing Django's utilities.
            with self.connection.cursor() as cursor:
                cursor.execute('SELECT current_version()')
        except Database.Error:
            return False
        else:
            return True


# class DatabaseWrapper(BaseDatabaseWrapper):
#     vendor = 'snowflake'
#     display_name = 'Snowflake'
#     # This dictionary maps Field objects to their associated PostgreSQL column
#     # types, as strings. Column-type strings can contain format strings; they'll
#     # be interpolated against the values of Field.__dict__ before being output.
#     # If a column type is set to None, it won't be included in the output.
#     data_types = {
#         'AutoField': 'serial',
#         'BigAutoField': 'bigserial',
#         'BinaryField': 'bytea',
#         'BooleanField': 'boolean',
#         'CharField': 'varchar(%(max_length)s)',
#         'DateField': 'date',
#         'DateTimeField': 'timestamp with time zone',
#         'DecimalField': 'numeric(%(max_digits)s, %(decimal_places)s)',
#         'DurationField': 'interval',
#         'FileField': 'varchar(%(max_length)s)',
#         'FilePathField': 'varchar(%(max_length)s)',
#         'FloatField': 'double precision',
#         'IntegerField': 'integer',
#         'BigIntegerField': 'bigint',
#         'IPAddressField': 'inet',
#         'GenericIPAddressField': 'inet',
#         'JSONField': 'jsonb',
#         'NullBooleanField': 'boolean',
#         'OneToOneField': 'integer',
#         'PositiveBigIntegerField': 'bigint',
#         'PositiveIntegerField': 'integer',
#         'PositiveSmallIntegerField': 'smallint',
#         'SlugField': 'varchar(%(max_length)s)',
#         'SmallAutoField': 'smallserial',
#         'SmallIntegerField': 'smallint',
#         'TextField': 'text',
#         'TimeField': 'time',
#         'UUIDField': 'uuid',
#     }
#     data_type_check_constraints = {
#         'PositiveBigIntegerField': '"%(column)s" >= 0',
#         'PositiveIntegerField': '"%(column)s" >= 0',
#         'PositiveSmallIntegerField': '"%(column)s" >= 0',
#     }
#     operators = {
#         'exact': '= %s',
#         'iexact': '= UPPER(%s)',
#         'contains': 'LIKE %s',
#         'icontains': 'LIKE UPPER(%s)',
#         'regex': '~ %s',
#         'iregex': '~* %s',
#         'gt': '> %s',
#         'gte': '>= %s',
#         'lt': '< %s',
#         'lte': '<= %s',
#         'startswith': 'LIKE %s',
#         'endswith': 'LIKE %s',
#         'istartswith': 'LIKE UPPER(%s)',
#         'iendswith': 'LIKE UPPER(%s)',
#     }
#
#     # The patterns below are used to generate SQL pattern lookup clauses when
#     # the right-hand side of the lookup isn't a raw string (it might be an expression
#     # or the result of a bilateral transformation).
#     # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
#     # escaped on database side.
#     #
#     # Note: we use str.format() here for readability as '%' is used as a wildcard for
#     # the LIKE operator.
#     pattern_esc = r"REPLACE(REPLACE(REPLACE({}, E'\\', E'\\\\'), E'%%', E'\\%%'), E'_', E'\\_')"
#     pattern_ops = {
#         'contains': "LIKE '%%' || {} || '%%'",
#         'icontains': "LIKE '%%' || UPPER({}) || '%%'",
#         'startswith': "LIKE {} || '%%'",
#         'istartswith': "LIKE UPPER({}) || '%%'",
#         'endswith': "LIKE '%%' || {}",
#         'iendswith': "LIKE '%%' || UPPER({})",
#     }
#
#     Database = Database
#     SchemaEditorClass = DatabaseSchemaEditor
#     # Classes instantiated in __init__().
#     client_class = DatabaseClient
#     creation_class = DatabaseCreation
#     features_class = DatabaseFeatures
#     introspection_class = DatabaseIntrospection
#     ops_class = DatabaseOperations
#     # PostgreSQL backend-specific attributes.
#     _named_cursor_idx = 0
#
#     def get_connection_params(self):
#         settings_dict = self.settings_dict
#
#         if settings_dict['DATABASE'] == '' or None:
#             raise ImproperlyConfigured("settings.DATABASES is improperly configured - Please supply the NAME value.")
#
#         # TODO: update for snowflake max db name length
#         # if len(settings_dict['DATABASE']) > self.ops.max_name_length():
#         #     raise ImproperlyConfigured(
#         #         "The database name '%s' (%d characters) is longer than "
#         #         "Snowflake's limit of %d characters. Supply a shorter NAME "
#         #         "in settings.DATABASES." % (
#         #             settings_dict['DATABASE'],
#         #             len(settings_dict['DATABASE']),
#         #             self.ops.max_name_length(),
#         #         )
#         #     )
#
#         conn_params = {
#             'database': settings_dict['DATABASE'],
#             #**settings_dict['OPTIONS'],
#         }
#
#         conn_params.pop('isolation_level', None)
#
#         if settings_dict['USER']:
#             conn_params['user'] = settings_dict['USER']
#         else:
#             raise ImproperlyConfigured("Please provide a username for snowflake!")
#
#         if settings_dict['PASSWORD']:
#             conn_params['password'] = settings_dict['PASSWORD']
#         else:
#             raise ImproperlyConfigured("Please provide a password for snowflake!")
#
#         if settings_dict['ACCOUNT']:
#             conn_params['account'] = settings_dict['ACCOUNT']
#         else:
#             raise ImproperlyConfigured("Please provide an account for snowflake!")
#
#         if settings_dict['WAREHOUSE']:
#             conn_params['warehouse'] = settings_dict['WAREHOUSE']
#         else:
#             raise ImproperlyConfigured("Please provide a warehouse for snowflake!")
#
#         if settings_dict['ROLE']:
#             conn_params['role'] = settings_dict['ROLE']
#         else:
#             raise ImproperlyConfigured("Please provide a role for snowflake!")
#
#         if settings_dict['SCHEMA']:
#             conn_params['schema'] = settings_dict['SCHEMA']
#         else:
#             raise ImproperlyConfigured("Please provide a schema for snowflake!")
#
#         return conn_params
#
#     @async_unsafe
#     def get_new_connection(self, conn_params):
#         connection = Database.connect(**conn_params)
#         return connection
#
#     def ensure_timezone(self):
#         if self.connection is None:
#             return False
#         conn_timezone_name = self.connection.get_parameter_status('TimeZone')
#         timezone_name = self.timezone_name
#         if timezone_name and conn_timezone_name != timezone_name:
#             with self.connection.cursor() as cursor:
#                 cursor.execute(self.ops.set_time_zone_sql(), [timezone_name])
#             return True
#         return False
#
#     def init_connection_state(self):
#         self.connection.set_client_encoding('UTF8')
#
#         timezone_changed = self.ensure_timezone()
#         if timezone_changed:
#             # Commit after setting the time zone (see #17062)
#             if not self.get_autocommit():
#                 self.connection.commit()
#
#     @async_unsafe
#     def create_cursor(self, name=None):
#         if name:
#             # In autocommit mode, the cursor will be used outside of a
#             # transaction, hence use a holdable cursor.
#             cursor = self.connection.cursor(name, scrollable=False, withhold=self.connection.autocommit)
#         else:
#             cursor = self.connection.cursor()
#         cursor.tzinfo_factory = self.tzinfo_factory if settings.USE_TZ else None
#         return cursor
#
#     def tzinfo_factory(self, offset):
#         return self.timezone
#
#     @async_unsafe
#     def chunked_cursor(self):
#         self._named_cursor_idx += 1
#         # Get the current async task
#         # Note that right now this is behind @async_unsafe, so this is
#         # unreachable, but in future we'll start loosening this restriction.
#         # For now, it's here so that every use of "threading" is
#         # also async-compatible.
#         try:
#             if hasattr(asyncio, 'current_task'):
#                 # Python 3.7 and up
#                 current_task = asyncio.current_task()
#             else:
#                 # Python 3.6
#                 current_task = asyncio.Task.current_task()
#         except RuntimeError:
#             current_task = None
#         # Current task can be none even if the current_task call didn't error
#         if current_task:
#             task_ident = str(id(current_task))
#         else:
#             task_ident = 'sync'
#         # Use that and the thread ident to get a unique name
#         return self._cursor(
#             name='_django_curs_%d_%s_%d' % (
#                 # Avoid reusing name in other threads / tasks
#                 threading.current_thread().ident,
#                 task_ident,
#                 self._named_cursor_idx,
#             )
#         )
#
#     def _set_autocommit(self, autocommit):
#         with self.wrap_database_errors:
#             self.connection.autocommit = autocommit
#
#     def check_constraints(self, table_names=None):
#         """
#         Check constraints by setting them to immediate. Return them to deferred
#         afterward.
#         """
#         with self.cursor() as cursor:
#             cursor.execute('SET CONSTRAINTS ALL IMMEDIATE')
#             cursor.execute('SET CONSTRAINTS ALL DEFERRED')
#
#     def is_usable(self):
#         try:
#             # Use a psycopg cursor directly, bypassing Django's utilities.
#             with self.connection.cursor() as cursor:
#                 cursor.execute('SELECT 1')
#         except Database.Error:
#             return False
#         else:
#             return True
#
#     @contextmanager
#     def _nodb_cursor(self):
#         try:
#             with super()._nodb_cursor() as cursor:
#                 yield cursor
#         except (Database.DatabaseError, WrappedDatabaseError):
#             warnings.warn(
#                 "Normally Django will use a connection to the 'postgres' database "
#                 "to avoid running initialization queries against the production "
#                 "database when it's not needed (for example, when running tests). "
#                 "Django was unable to create a connection to the 'postgres' database "
#                 "and will use the first PostgreSQL database instead.",
#                 RuntimeWarning
#             )
#             for connection in connections.all():
#                 if connection.vendor == 'postgresql' and connection.settings_dict['NAME'] != 'postgres':
#                     conn = self.__class__(
#                         {**self.settings_dict, 'NAME': connection.settings_dict['NAME']},
#                         alias=self.alias,
#                     )
#                     try:
#                         with conn.cursor() as cursor:
#                             yield cursor
#                     finally:
#                         conn.close()
#
#     @cached_property
#     def pg_version(self):
#         with self.temporary_connection():
#             return self.connection.server_version
#
#     def make_debug_cursor(self, cursor):
#         return CursorDebugWrapper(cursor, self)


class CursorDebugWrapper(BaseCursorDebugWrapper):
    def copy_expert(self, sql, file, *args):
        with self.debug_sql(sql):
            return self.cursor.copy_expert(sql, file, *args)

    def copy_to(self, file, table, *args, **kwargs):
        with self.debug_sql(sql='COPY %s TO STDOUT' % table):
            return self.cursor.copy_to(file, table, *args, **kwargs)
