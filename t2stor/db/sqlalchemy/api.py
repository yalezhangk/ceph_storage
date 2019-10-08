try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

import functools
import itertools
import re
import sys
import uuid

from oslo_config import cfg
from oslo_db import api as oslo_db_api
from oslo_db import exception as db_exc
from oslo_db import options
from oslo_db.sqlalchemy import enginefacade
from oslo_log import log as logging
from oslo_utils import timeutils
import six
import sqlalchemy
from sqlalchemy import or_, and_, case
from sqlalchemy.orm import RelationshipProperty
from sqlalchemy import sql
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.sql import func

from t2stor.common import sqlalchemyutils
from t2stor import db
from t2stor.db.sqlalchemy import models
from t2stor import exception
from t2stor.i18n import _

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

options.set_defaults(CONF, connection='sqlite:///$state_path/stor.sqlite')

main_context_manager = enginefacade.transaction_context()


def configure(conf):
    main_context_manager.configure(**dict(conf.database))


def get_engine(use_slave=False):
    return main_context_manager._factory.get_legacy_facade().get_engine(
        use_slave=use_slave)


def get_session(use_slave=False, **kwargs):
    return main_context_manager._factory.get_legacy_facade().get_session(
        use_slave=use_slave, **kwargs)


def dispose_engine():
    get_engine().dispose()


_DEFAULT_QUOTA_NAME = 'default'


def get_backend():
    """The backend is this module itself."""

    return sys.modules[__name__]


def is_admin_context(context):
    """Indicates if the request context is an administrator."""
    if not context:
        raise exception.StorException(
            'Use of empty request context is deprecated')
    return context.is_admin


def is_user_context(context):
    """Indicates if the request context is a normal user."""
    if not context:
        return False
    if context.is_admin:
        return False
    if not context.user_id or not context.project_id:
        return False
    return True


def authorize_project_context(context, project_id):
    """Ensures a request has permission to access the given project."""
    if is_user_context(context):
        if not context.project_id:
            raise exception.NotAuthorized()
        elif context.project_id != project_id:
            raise exception.NotAuthorized()


def authorize_user_context(context, user_id):
    """Ensures a request has permission to access the given user."""
    if is_user_context(context):
        if not context.user_id:
            raise exception.NotAuthorized()
        elif context.user_id != user_id:
            raise exception.NotAuthorized()


def authorize_quota_class_context(context, class_name):
    """Ensures a request has permission to access the given quota class."""
    if is_user_context(context):
        if not context.quota_class:
            raise exception.NotAuthorized()
        elif context.quota_class != class_name:
            raise exception.NotAuthorized()


def require_admin_context(f):
    """Decorator to require admin request context.

    The first argument to the wrapped function must be the context.

    """

    def wrapper(*args, **kwargs):
        if not is_admin_context(args[0]):
            raise exception.AdminRequired()
        return f(*args, **kwargs)
    return wrapper


def require_context(f):
    """Decorator to require *any* user or admin context.

    This does no authorization for user or project access matching, see
    :py:func:`authorize_project_context` and
    :py:func:`authorize_user_context`.

    The first argument to the wrapped function must be the context.

    """

    def wrapper(*args, **kwargs):
        if not is_admin_context(args[0]) and not is_user_context(args[0]):
            raise exception.NotAuthorized()
        return f(*args, **kwargs)
    return wrapper


def require_volume_exists(f):
    """Decorator to require the specified volume to exist.

    Requires the wrapped function to use context and volume_id as
    their first two arguments.
    """

    @functools.wraps(f)
    def wrapper(context, volume_id, *args, **kwargs):
        if not resource_exists(context, models.Volume, volume_id):
            raise exception.VolumeNotFound(volume_id=volume_id)
        return f(context, volume_id, *args, **kwargs)
    return wrapper


def require_snapshot_exists(f):
    """Decorator to require the specified snapshot to exist.

    Requires the wrapped function to use context and snapshot_id as
    their first two arguments.
    """

    @functools.wraps(f)
    def wrapper(context, snapshot_id, *args, **kwargs):
        if not resource_exists(context, models.Snapshot, snapshot_id):
            raise exception.SnapshotNotFound(snapshot_id=snapshot_id)
        return f(context, snapshot_id, *args, **kwargs)
    return wrapper


def require_backup_exists(f):
    """Decorator to require the specified snapshot to exist.

    Requires the wrapped function to use context and backup_id as
    their first two arguments.
    """

    @functools.wraps(f)
    def wrapper(context, backup_id, *args, **kwargs):
        if not resource_exists(context, models.Backup, backup_id):
            raise exception.BackupNotFound(backup_id=backup_id)
        return f(context, backup_id, *args, **kwargs)
    return wrapper


def handle_db_data_error(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except db_exc.DBDataError:
            msg = _('Error writing field to database')
            LOG.exception(msg)
            raise exception.Invalid(msg)

    return wrapper


def model_query(context, model, *args, **kwargs):
    """Query helper that accounts for context's `read_deleted` field.

    :param context:      context to query under
    :param model:        Model to query. Must be a subclass of ModelBase.
    :param args:         Arguments to query. If None - model is used.
    :param read_deleted: if present, overrides context's read_deleted field.
    :param project_only: if present and context is user-type, then restrict
                         query to match the context's project_id.
    """
    session = kwargs.get('session') or get_session()
    read_deleted = kwargs.get('read_deleted') or context.read_deleted

    query = session.query(model, *args)

    if read_deleted == 'no':
        query = query.filter_by(deleted=False)
    elif read_deleted == 'yes':
        pass  # omit the filter to include deleted and active
    elif read_deleted == 'only':
        query = query.filter_by(deleted=True)
    elif read_deleted == 'int_no':
        query = query.filter_by(deleted=0)
    else:
        raise Exception(
            _("Unrecognized read_deleted value '%s'") % read_deleted)

    query = query.filter_by()

    return query


###################


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_create(context, values):

    volume_ref = models.Volume
    if not values.get('id'):
        values['id'] = str(uuid.uuid4())
    volume_ref.update(values)

    session = get_session()
    with session.begin():
        session.add(volume_ref)

    return _volume_get(context, values['id'], session=session)


@require_admin_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_destroy(context, volume_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'status': 'deleted',
                          'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Volume, session=session).\
            filter_by(id=volume_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _process_model_like_filter(model, query, filters):
    """Applies regex expression filtering to a query.

    :param model: model to apply filters to
    :param query: query to apply filters to
    :param filters: dictionary of filters with regex values
    :returns: the updated query.
    """
    if query is None:
        return query

    for key in sorted(filters):
        column_attr = getattr(model, key)
        if 'property' == type(column_attr).__name__:
            continue
        value = filters[key]
        if not (isinstance(value, (six.string_types, int))):
            continue
        query = query.filter(
            column_attr.op('LIKE')(u'%%%s%%' % value))
    return query


def apply_like_filters(model):
    def decorator_filters(process_exact_filters):
        def _decorator(context, query, filters):
            exact_filters = filters.copy()
            regex_filters = {}
            for key, value in filters.items():
                # NOTE(tommylikehu): For inexact match, the filter keys
                # are in the format of 'key~=value'
                if key.endswith('~'):
                    exact_filters.pop(key)
                    regex_filters[key.rstrip('~')] = value
            query = process_exact_filters(context, query, exact_filters)
            return _process_model_like_filter(model, query, regex_filters)
        return _decorator
    return decorator_filters


def _process_volume_filters(query, filters):
    filters = filters.copy()
    for key in filters.keys():
        try:
            column_attr = getattr(models.Volume, key)
            # Do not allow relationship properties since those require
            # schema specific knowledge
            prop = getattr(column_attr, 'property')
            if isinstance(prop, RelationshipProperty):
                LOG.debug(("'%s' filter key is not valid, "
                           "it maps to a relationship."), key)
                return None
        except AttributeError:
            LOG.debug("'%s' filter key is not valid.", key)
            return None

    # Holds the simple exact matches
    filter_dict = {}

    # Iterate over all filters, special case the filter if necessary
    for key, value in filters.items():
        if isinstance(value, (list, tuple, set, frozenset)):
            # Looking for values in a list; apply to query directly
            column_attr = getattr(models.Volume, key)
            query = query.filter(column_attr.in_(value))
        else:
            # OK, simple exact match; save for later
            filter_dict[key] = value

    # Apply simple exact matches
    if filter_dict:
        query = query.filter_by(**filter_dict)
    return query


@require_context
def _volume_get_query(context, session=None, project_only=False,
                      joined_load=True):
    """Get the query to retrieve the volume.

    :param context: the context used to run the method _volume_get_query
    :param session: the session to use
    :param project_only: the boolean used to decide whether to query the
                         volume in the current project or all projects
    :param joined_load: the boolean used to decide whether the query loads
                        the other models, which join the volume model in
                        the database. Currently, the False value for this
                        parameter is specially for the case of updating
                        database during volume migration
    :returns: updated query or None
    """
    return model_query(context, models.Volume, session=session,
                       project_only=project_only)


@require_context
def _volume_get(context, volume_id, session=None, joined_load=True):
    result = _volume_get_query(context, session=session, project_only=True,
                               joined_load=joined_load)
    result = result.filter_by(id=volume_id).first()

    if not result:
        raise exception.VolumeNotFound(volume_id=volume_id)

    return result


@require_context
def volume_get(context, volume_id):
    return _volume_get(context, volume_id)


@require_context
def volume_get_all(context, marker=None, limit=None, sort_keys=None,
                   sort_dirs=None, filters=None, offset=None):
    """Retrieves all volumes.

    If no sort parameters are specified then the returned volumes are sorted
    first by the 'created_at' key and then by the 'id' key in descending
    order.

    :param context: context to query under
    :param marker: the last item of the previous page, used to determine the
                   next page of results to return
    :param limit: maximum number of items to return
    :param sort_keys: list of attributes by which results should be sorted,
                      paired with corresponding item in sort_dirs
    :param sort_dirs: list of directions in which results should be sorted,
                      paired with corresponding item in sort_keys
    :param filters: dictionary of filters; values that are in lists, tuples,
                    or sets cause an 'IN' operation, while exact matching
                    is used for other values, see _process_volume_filters
                    function for more information
    :returns: list of matching volumes
    """
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Volume,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No volumes would match, return empty list
        if query is None:
            return []
        return query.all()


def _generate_paginate_query(context, session, model, marker, limit, sort_keys,
                             sort_dirs, filters, offset=None):
    """Generate the query to include the filters and the paginate options.

    Returns a query with sorting / pagination criteria added or None
    if the given filters will not yield any results.

    :param context: context to query under
    :param session: the session to use
    :param model: db model
    :param marker: the last item of the previous page; we returns the next
                    results after this value.
    :param limit: maximum number of items to return
    :param sort_keys: list of attributes by which results should be sorted,
                      paired with corresponding item in sort_dirs
    :param sort_dirs: list of directions in which results should be sorted,
                      paired with corresponding item in sort_keys
    :param filters: dictionary of filters; values that are in lists, tuples,
                    or sets cause an 'IN' operation, while exact matching
                    is used for other values, see _process_volume_filters
                    function for more information
    :param offset: number of items to skip
    :returns: updated query or None
    """
    get_query, process_filters, get = PAGINATION_HELPERS[model]

    sort_keys, sort_dirs = process_sort_params(sort_keys,
                                               sort_dirs,
                                               default_dir='desc')
    query = get_query(context, session=session)

    if filters:
        query = process_filters(query, filters)
        if query is None:
            return None

    marker_object = None
    if marker is not None:
        marker_object = get(context, marker, session)

    return sqlalchemyutils.paginate_query(query, model, limit,
                                          sort_keys,
                                          marker=marker_object,
                                          sort_dirs=sort_dirs,
                                          offset=offset)


CALCULATE_COUNT_HELPERS = {
}


def calculate_resource_count(context, resource_type, filters):
    """Calculate total count with filters applied"""

    session = get_session()
    if resource_type not in CALCULATE_COUNT_HELPERS.keys():
        raise exception.InvalidInput(
            reason=_("Model %s doesn't support "
                     "counting resource.") % resource_type)
    get_query, process_filters = CALCULATE_COUNT_HELPERS[resource_type]
    query = get_query(context, session=session)
    if filters:
        query = process_filters(query, filters)
        if query is None:
            return 0
    return query.with_entities(func.count()).scalar()


def process_sort_params(sort_keys, sort_dirs, default_keys=None,
                        default_dir='asc'):
    """Process the sort parameters to include default keys.

    Creates a list of sort keys and a list of sort directions. Adds the default
    keys to the end of the list if they are not already included.

    When adding the default keys to the sort keys list, the associated
    direction is:
    1) The first element in the 'sort_dirs' list (if specified), else
    2) 'default_dir' value (Note that 'asc' is the default value since this is
    the default in sqlalchemy.utils.paginate_query)

    :param sort_keys: List of sort keys to include in the processed list
    :param sort_dirs: List of sort directions to include in the processed list
    :param default_keys: List of sort keys that need to be included in the
                         processed list, they are added at the end of the list
                         if not already specified.
    :param default_dir: Sort direction associated with each of the default
                        keys that are not supplied, used when they are added
                        to the processed list
    :returns: list of sort keys, list of sort directions
    :raise exception.InvalidInput: If more sort directions than sort keys
                                   are specified or if an invalid sort
                                   direction is specified
    """
    if default_keys is None:
        default_keys = ['created_at', 'id']

    # Determine direction to use for when adding default keys
    if sort_dirs and len(sort_dirs):
        default_dir_value = sort_dirs[0]
    else:
        default_dir_value = default_dir

    # Create list of keys (do not modify the input list)
    if sort_keys:
        result_keys = list(sort_keys)
    else:
        result_keys = []

    # If a list of directions is not provided, use the default sort direction
    # for all provided keys.
    if sort_dirs:
        result_dirs = []
        # Verify sort direction
        for sort_dir in sort_dirs:
            if sort_dir not in ('asc', 'desc'):
                msg = _("Unknown sort direction, must be 'desc' or 'asc'.")
                raise exception.InvalidInput(reason=msg)
            result_dirs.append(sort_dir)
    else:
        result_dirs = [default_dir_value for _sort_key in result_keys]

    # Ensure that the key and direction length match
    while len(result_dirs) < len(result_keys):
        result_dirs.append(default_dir_value)
    # Unless more direction are specified, which is an error
    if len(result_dirs) > len(result_keys):
        msg = _("Sort direction array size exceeds sort key array size.")
        raise exception.InvalidInput(reason=msg)

    # Ensure defaults are included
    for key in default_keys:
        if key not in result_keys:
            result_keys.append(key)
            result_dirs.append(default_dir_value)

    return result_keys, result_dirs


@handle_db_data_error
@require_context
def volume_update(context, volume_id, values):
    session = get_session()
    with session.begin():
        query = _volume_get_query(context, session, joined_load=False)
        result = query.filter_by(id=volume_id).update(values)
        if not result:
            raise exception.VolumeNotFound(volume_id=volume_id)


@handle_db_data_error
@require_context
def volumes_update(context, values_list):
    session = get_session()
    with session.begin():
        volume_refs = []
        for values in values_list:
            volume_id = values['id']
            values.pop('id')
            volume_ref = _volume_get(context, volume_id, session=session)
            volume_ref.update(values)
            volume_refs.append(volume_ref)

        return volume_refs


###############################

@apply_like_filters(model=models.Cluster)
def _process_cluster_filters(query, filters):
    if filters:
        filters_dict = {}
        for key, value in filters.items():
            filters_dict[key] = value

        if filters_dict:
            query = query.filter_by(**filters_dict)
    return query


@require_context
def _cluster_get_query(context, session=None):
    return model_query(context, models.Cluster, session=session)


@require_context
def _cluster_get(context, cluster_id, session=None, joined_load=True):
    result = _cluster_get_query(context, session=session)
    result = result.filter_by(id=cluster_id).first()

    if not result:
        raise exception.ClusterNotFound(cluster_id=cluster_id)

    return result


@require_context
def cluster_get(context, cluster_id):
    return _cluster_get(context, cluster_id)


@require_context
def cluster_get_all(context, marker=None, limit=None, sort_keys=None,
                    sort_dirs=None, filters=None, offset=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Cluster, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@handle_db_data_error
@require_context
def cluster_update(context, cluster_id, values):
    session = get_session()
    with session.begin():
        query = _cluster_get_query(context, session, joined_load=False)
        result = query.filter_by(id=cluster_id).update(values)
        if not result:
            raise exception.ClusterNotFound(cluster_id=cluster_id)


@handle_db_data_error
@require_context
def clusters_update(context, values_list):
    session = get_session()
    with session.begin():
        cluster_refs = []
        for values in values_list:
            cluster_id = values['id']
            values.pop('id')
            cluster_ref = _cluster_get(context, cluster_id, session=session)
            cluster_ref.update(values)
            cluster_refs.append(cluster_ref)

        return cluster_refs


@require_context
def cluster_check_table_id(context, table_id):
    clusters = cluster_get_all(context, filters={"table_id": table_id})
    if clusters:
        return True
    return False


@require_context
def cluster_get_new_uuid(context):
    while True:
        uid = str(uuid.uuid4())
        table_id = uid[0:8]
        if cluster_check_table_id(context, table_id):
            continue
        return uid


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def cluster_create(context, values):

    if not values.get('id'):
        uid = cluster_get_new_uuid(context)
        values['id'] = uid
        values['table_id'] = uid[0:8]
    else:
        uid = values.get('id')
        values['table_id'] = uid[0:8]
        if cluster_check_table_id(context, values['table_id']):
            raise exception.ClusterExists(cluster_id=uid)

    cluster_ref = models.Cluster()
    cluster_ref.update(values)
    session = get_session()
    with session.begin():
        session.add(cluster_ref)

    return _cluster_get(context, values['id'], session=session)


###############################


def _rpc_service_get_query(context, session=None):
    return model_query(context, models.RPCService, session=session)


def _rpc_service_get(context, rpc_service_id, session=None):
    result = _rpc_service_get_query(context, session)
    result = result.filter_by(id=rpc_service_id).first()

    if not result:
        raise exception.RPCServiceNotFound(rpc_service_id=rpc_service_id)

    return result


@require_context
def rpc_service_create(context, values):
    rpc_service_ref = models.RPCService()
    rpc_service_ref.update(values)
    session = get_session()
    with session.begin():
        session.add(rpc_service_ref)

    return _rpc_service_get(context, values['id'], session=session)


def rpc_service_destroy(context, rpc_service_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.RPCService, session=session).\
            filter_by(id=rpc_service_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def rpc_service_get(context, rpc_service_id):
    return _rpc_service_get(context, rpc_service_id)


@require_context
def rpc_service_get_all(context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.RPCService, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
def rpc_service_update(context, rpc_service_id, values):
    session = get_session()
    with session.begin():
        query = _rpc_service_get_query(context, session)
        result = query.filter_by(id=rpc_service_id).update(values)
        if not result:
            raise exception.RPCServiceNotFound(rpc_service_id=rpc_service_id)


###############################


def _node_get_query(context, session=None):
    return model_query(context, models.Node, session=session)


def _node_get(context, node_id, session=None):
    result = _node_get_query(context, session)
    result = result.filter_by(id=node_id).first()

    if not result:
        raise exception.NodeNotFound(node_id=node_id)

    return result


@require_context
def node_create(context, values):
    node_ref = models.Node()
    node_ref.update(values)
    session = get_session()
    with session.begin():
        session.add(node_ref)

    return _node_get(context, values['id'], session=session)


def node_destroy(context, node_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Node, session=session).\
            filter_by(id=node_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def node_get(context, node_id):
    return _node_get(context, node_id)


@require_context
def node_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Node, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
def node_update(context, node_id, values):
    session = get_session()
    with session.begin():
        query = _node_get_query(context, session)
        result = query.filter_by(id=node_id).update(values)
        if not result:
            raise exception.NodeNotFound(node_id=node_id)


###############################


def _datacenter_get_query(context, session=None):
    return model_query(context, models.Datacenter, session=session)


def _datacenter_get(context, datacenter_id, session=None):
    result = _datacenter_get_query(context, session)
    result = result.filter_by(id=datacenter_id).first()

    if not result:
        raise exception.DatacenterNotFound(datacenter_id=datacenter_id)

    return result


@require_context
def datacenter_create(context, values):
    datacenter_ref = models.Datacenter()
    datacenter_ref.update(values)
    session = get_session()
    with session.begin():
        session.add(datacenter_ref)

    return _datacenter_get(context, values['id'], session=session)


def datacenter_destroy(context, datacenter_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Datacenter, session=session).\
            filter_by(id=datacenter_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def datacenter_get(context, datacenter_id):
    return _datacenter_get(context, datacenter_id)


@require_context
def datacenter_get_all(context, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Datacenter, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
def datacenter_update(context, datacenter_id, values):
    session = get_session()
    with session.begin():
        query = _datacenter_get_query(context, session)
        result = query.filter_by(id=datacenter_id).update(values)
        if not result:
            raise exception.DatacenterNotFound(datacenter_id=datacenter_id)


###############################


def _rack_get_query(context, session=None):
    return model_query(context, models.Rack, session=session)


def _rack_get(context, rack_id, session=None):
    result = _rack_get_query(context, session)
    result = result.filter_by(id=rack_id).first()

    if not result:
        raise exception.RackNotFound(rack_id=rack_id)

    return result


@require_context
def rack_create(context, values):
    rack_ref = models.Rack()
    rack_ref.update(values)
    session = get_session()
    with session.begin():
        session.add(rack_ref)

    return _rack_get(context, values['id'], session=session)


def rack_destroy(context, rack_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Rack, session=session).\
            filter_by(id=rack_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def rack_get(context, rack_id):
    return _rack_get(context, rack_id)


@require_context
def rack_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Rack, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
def rack_update(context, rack_id, values):
    session = get_session()
    with session.begin():
        query = _rack_get_query(context, session)
        result = query.filter_by(id=rack_id).update(values)
        if not result:
            raise exception.RackNotFound(rack_id=rack_id)


###############################


def _osd_get_query(context, session=None):
    return model_query(context, models.Osd, session=session)


def _osd_get(context, osd_id, session=None):
    result = _osd_get_query(context, session)
    result = result.filter_by(id=osd_id).first()

    if not result:
        raise exception.OsdNotFound(osd_id=osd_id)

    return result


@require_context
def osd_create(context, values):
    osd_ref = models.Osd()
    osd_ref.update(values)
    session = get_session()
    with session.begin():
        session.add(osd_ref)

    return _osd_get(context, values['id'], session=session)


def osd_destroy(context, osd_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Osd, session=session).\
            filter_by(id=osd_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def osd_get(context, osd_id):
    return _osd_get(context, osd_id)


@require_context
def osd_get_all(context, marker=None, limit=None, sort_keys=None,
                sort_dirs=None, filters=None, offset=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Osd, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
def osd_update(context, osd_id, values):
    session = get_session()
    with session.begin():
        query = _osd_get_query(context, session)
        result = query.filter_by(id=osd_id).update(values)
        if not result:
            raise exception.OsdNotFound(osd_id=osd_id)


###############################


def is_valid_model_filters(model, filters, exclude_list=None):
    """Return True if filter values exist on the model

    :param model: a model
    :param filters: dictionary of filters
    """
    for key in filters.keys():
        if exclude_list and key in exclude_list:
            continue
        key = key.rstrip('~')
        if not hasattr(model, key):
            LOG.debug("'%s' filter key is not valid.", key)
            return False
    return True


def process_filters(model):
    def _process_filters(query, filters):
        if filters:
            # Ensure that filters' keys exist on the model
            if not is_valid_model_filters(model, filters):
                return

            # Apply exact matches
            if filters:
                query = query.filter_by(**filters)
        return query
    return _process_filters


PAGINATION_HELPERS = {
    models.Volume: (_volume_get_query, _process_volume_filters, _volume_get),
    models.Cluster: (_cluster_get_query, _process_cluster_filters,
                     _cluster_get),
    models.RPCService: (_rpc_service_get_query,
                        process_filters(models.RPCService), _rpc_service_get),

}


@require_context
def resource_exists(context, model, resource_id, session=None):
    conditions = [model.id == resource_id]
    # Match non deleted resources by the id
    if 'no' == context.read_deleted:
        conditions.append(~model.deleted)
    # If the context is not admin we limit it to the context's project
    if is_user_context(context) and hasattr(model, 'project_id'):
        conditions.append(model.project_id == context.project_id)
    session = session or get_session()
    query = session.query(sql.exists().where(and_(*conditions)))
    return query.scalar()


def _get_get_method(model):
    # Exceptions to model to get methods, in general method names are a simple
    # conversion changing ORM name from camel case to snake format and adding
    # _get to the string
    GET_EXCEPTIONS = {
    }

    if model in GET_EXCEPTIONS:
        return GET_EXCEPTIONS[model]

    # General conversion
    # Convert camel cased model name to snake format
    s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', model)
    # Get method must be snake formatted model name concatenated with _get
    method_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower() + '_get'
    return globals().get(method_name)


_GET_METHODS = {}


@require_context
def get_by_id(context, model, id, *args, **kwargs):
    # Add get method to cache dictionary if it's not already there
    if not _GET_METHODS.get(model):
        _GET_METHODS[model] = _get_get_method(model)

    return _GET_METHODS[model](context, id, *args, **kwargs)


def condition_db_filter(model, field, value):
    """Create matching filter.

    If value is an iterable other than a string, any of the values is
    a valid match (OR), so we'll use SQL IN operator.

    If it's not an iterator == operator will be used.
    """
    orm_field = getattr(model, field)
    # For values that must match and are iterables we use IN
    if (isinstance(value, Iterable) and
            not isinstance(value, six.string_types)):
        # We cannot use in_ when one of the values is None
        if None not in value:
            return orm_field.in_(value)

        return or_(orm_field == v for v in value)

    # For values that must match and are not iterables we use ==
    return orm_field == value


def condition_not_db_filter(model, field, value, auto_none=True):
    """Create non matching filter.

    If value is an iterable other than a string, any of the values is
    a valid match (OR), so we'll use SQL IN operator.

    If it's not an iterator == operator will be used.

    If auto_none is True then we'll consider NULL values as different as well,
    like we do in Python and not like SQL does.
    """
    result = ~condition_db_filter(model, field, value)

    if (auto_none
            and ((isinstance(value, Iterable) and
                  not isinstance(value, six.string_types)
                  and None not in value)
                 or (value is not None))):
        orm_field = getattr(model, field)
        result = or_(result, orm_field.is_(None))

    return result


def is_orm_value(obj):
    """Check if object is an ORM field or expression."""
    return isinstance(obj, (sqlalchemy.orm.attributes.InstrumentedAttribute,
                            sqlalchemy.sql.expression.ColumnElement))


def _check_is_not_multitable(values, model):
    """Check that we don't try to do multitable updates.

    Since PostgreSQL doesn't support multitable updates we want to always fail
    if we have such a query in our code, even if with MySQL it would work.
    """
    used_models = set()
    for field in values:
        if isinstance(field, sqlalchemy.orm.attributes.InstrumentedAttribute):
            used_models.add(field.class_)
        elif isinstance(field, six.string_types):
            used_models.add(model)
        else:
            raise exception.ProgrammingError(
                reason='DB Conditional update - Unknown field type, must be '
                       'string or ORM field.')
        if len(used_models) > 1:
            raise exception.ProgrammingError(
                reason='DB Conditional update - Error in query, multitable '
                       'updates are not supported.')


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def conditional_update(context, model, values, expected_values, filters=(),
                       include_deleted='no', project_only=False, order=None):
    """Compare-and-swap conditional update SQLAlchemy implementation."""
    _check_is_not_multitable(values, model)

    # Provided filters will become part of the where clause
    where_conds = list(filters)

    # Build where conditions with operators ==, !=, NOT IN and IN
    for field, condition in expected_values.items():
        if not isinstance(condition, db.Condition):
            condition = db.Condition(condition, field)
        where_conds.append(condition.get_filter(model, field))

    # Create the query with the where clause
    query = model_query(context, model, read_deleted=include_deleted,
                        project_only=project_only).filter(*where_conds)

    # NOTE(geguileo): Some DBs' update method are order dependent, and they
    # behave differently depending on the order of the values, example on a
    # volume with 'available' status:
    #    UPDATE volumes SET previous_status=status, status='reyping'
    #        WHERE id='44f284f9-877d-4fce-9eb4-67a052410054';
    # Will result in a volume with 'retyping' status and 'available'
    # previous_status both on SQLite and MariaDB, but
    #    UPDATE volumes SET status='retyping', previous_status=status
    #        WHERE id='44f284f9-877d-4fce-9eb4-67a052410054';
    # Will yield the same result in SQLite but will result in a volume with
    # status and previous_status set to 'retyping' in MariaDB, which is not
    # what we want, so order must be taken into consideration.
    # Order for the update will be:
    #  1- Order specified in argument order
    #  2- Values that refer to other ORM field (simple and using operations,
    #     like size + 10)
    #  3- Values that use Case clause (since they may be using fields as well)
    #  4- All other values
    order = list(order) if order else tuple()
    orm_field_list = []
    case_list = []
    unordered_list = []
    for key, value in values.items():
        if isinstance(value, db.Case):
            value = case(value.whens, value.value, value.else_)

        if key in order:
            order[order.index(key)] = (key, value)
            continue
        # NOTE(geguileo): Check Case first since it's a type of orm value
        if isinstance(value, sql.elements.Case):
            value_list = case_list
        elif is_orm_value(value):
            value_list = orm_field_list
        else:
            value_list = unordered_list
        value_list.append((key, value))

    update_args = {'synchronize_session': False}

    # If we don't have to enforce any kind of order just pass along the values
    # dictionary since it will be a little more efficient.
    if order or orm_field_list or case_list:
        # If we are doing an update with ordered parameters, we need to add
        # remaining values to the list
        values = itertools.chain(order, orm_field_list, case_list,
                                 unordered_list)
        # And we have to tell SQLAlchemy that we want to preserve the order
        update_args['update_args'] = {'preserve_parameter_order': True}

    # Return True if we were able to change any DB entry, False otherwise
    result = query.update(values, **update_args)
    return 0 != result
