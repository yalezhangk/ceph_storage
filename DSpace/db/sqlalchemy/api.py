try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

import functools
import itertools
import re
import sys
import uuid

import six
import sqlalchemy
from oslo_config import cfg
from oslo_db import api as oslo_db_api
from oslo_db import exception as db_exc
from oslo_db import options
from oslo_db.sqlalchemy import enginefacade
from oslo_log import log as logging
from oslo_utils import timeutils
from sqlalchemy import and_
from sqlalchemy import case
from sqlalchemy import or_
from sqlalchemy import sql
from sqlalchemy.orm import RelationshipProperty
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import literal_column

from DSpace import db
from DSpace import exception
from DSpace.common import sqlalchemyutils
from DSpace.db.sqlalchemy import models
from DSpace.i18n import _
from DSpace.objects.fields import ServiceStatus

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
    if not context.user_id:
        return False
    return True


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

    This does no authorization for user access matching, see
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
    volume_ref = models.Volume()
    volume_ref.update(values)

    session = get_session()
    with session.begin():
        volume_ref.save(session)

    return volume_ref


@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_destroy(context, access_path_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'status': 'deleted',
                          'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Volume, session=session). \
            filter_by(id=access_path_id). \
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
        value = filters[key]
        if not (isinstance(value, (six.string_types, int))):
            continue
        if key.endswith('~'):
            query = _get_query_filters_by_content(model, key, query, value)
        elif key.startswith('^'):
            query = _get_query_filters_by_startswith(model, key, query, value)
        else:
            continue
    return query


def _get_query_filters_by_content(model, key, query, value):
    key = key.rstrip('~')
    column_attr = getattr(model, key)
    if 'property' == type(column_attr).__name__:
        return query
    query = query.filter(
        column_attr.op('LIKE')(u'%%%s%%' % value))
    return query


def _get_query_filters_by_startswith(model, key, query, value):
    key = key.lstrip('^')
    column_attr = getattr(model, key)
    if 'property' == type(column_attr).__name__:
        return query
    query = query.filter(
        column_attr.op('LIKE')(u'%s%%' % value))
    return query


def apply_like_filters(model):
    def decorator_filters(process_exact_filters):
        def _decorator(query, filters):
            exact_filters = filters.copy()
            regex_filters = {}
            for key, value in filters.items():
                # NOTE(tommylikehu): For inexact match, the filter keys
                # are in the format of 'key~=value' or '^key=value'
                if key.endswith('~') or key.startswith('^'):
                    exact_filters.pop(key)
                    regex_filters[key] = value
            query = process_exact_filters(query, exact_filters)
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
def _volume_get_query(context, session=None):
    """Get the query to retrieve the volume.

    :param context: the context used to run the method _volume_get_query
    :param session: the session to use
    :returns: updated query or None
    """
    return model_query(context, models.Volume, session=session)


@require_context
def _volume_get(context, volume_id, session=None):
    result = _volume_get_query(context, session=session)
    result = result.filter_by(id=volume_id).first()

    if not result:
        raise exception.VolumeNotFound(volume_id=volume_id)

    return result


def _volume_load_attr(ctxt, volume, expected_attrs=None, session=None):
    expected_attrs = expected_attrs or []
    if 'snapshots' in expected_attrs:
        volume.snapshots = [snapshot for snapshot in volume._snapshots
                            if not snapshot.deleted]
    if 'pool' in expected_attrs:
        volume.pool = _pool_get(ctxt, volume.pool_id, session)
    if 'parent_snap' in expected_attrs:
        parent_snap_id = volume.snapshot_id
        if parent_snap_id and volume.is_link_clone:
            volume.parent_snap = _volume_snapshot_get(ctxt, parent_snap_id,
                                                      session)
        else:
            volume.parent_snap = None

    ac_path_id = None
    cli_group_ids = []
    mappings = _volume_mapping_get_query(
        ctxt, session).filter_by(volume_id=volume.id).all()
    if mappings:
        ac_path_id = mappings[0].volume_access_path_id
    for mapping in mappings:
        cli_group_id = mapping.volume_client_group_id
        if cli_group_id not in cli_group_ids:
            cli_group_ids.append(cli_group_id)

    if 'volume_client_groups' in expected_attrs:
        column_attr = getattr(models.VolumeClientGroup, "id")
        cgs = _volume_client_group_get_query(
            ctxt, session).filter(column_attr.in_(cli_group_ids))
        volume.volume_client_groups = [cg for cg in cgs]

    if 'volume_access_path' in expected_attrs:
        if ac_path_id:
            volume.volume_access_path = _volume_access_path_get(
                ctxt, ac_path_id, session)
        else:
            volume.volume_access_path = None

    if 'volume_clients' in expected_attrs:
        volume.volume_clients = []
        for vcg in volume.volume_client_groups:
            volume_clients = _volume_client_get_query(
                ctxt, session).filter_by(volume_client_group_id=vcg.id)
            volume.volume_clients.extend(volume_clients)


@require_context
def volume_get(context, volume_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        volume = _volume_get(context, volume_id, session)
        _volume_load_attr(context, volume, expected_attrs, session)
    return volume


@require_context
def volume_get_all(context, marker=None, limit=None, sort_keys=None,
                   sort_dirs=None, filters=None, offset=None,
                   expected_attrs=None):
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
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
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
        volumes = query.all()
        for volume in volumes:
            _volume_load_attr(context, volume, expected_attrs, session)
        return volumes


@require_context
def volume_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _volume_get_query(context, session)
        query = process_filters(models.Volume)(query, filters)
        return query.count()


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
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_update(context, volume_id, values):
    session = get_session()
    with session.begin():
        query = _volume_get_query(context, session)
        result = query.filter_by(id=volume_id).update(values)
        if not result:
            raise exception.VolumeNotFound(volume_id=volume_id)


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
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
def _cluster_get(context, cluster_id, session=None):
    result = _cluster_get_query(context, session=session)
    result = result.filter_by(id=cluster_id).first()

    if not result:
        raise exception.ClusterNotFound(cluster_id=cluster_id)

    return result


@require_context
def cluster_get(context, cluster_id, expected_attrs=None):
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


@require_context
def cluster_get_count(context, filters=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _cluster_get_query(context, session)
        query = process_filters(models.Cluster)(query, filters)
        return query.count()


@require_context
def node_status_get(context):
    session = get_session()
    filters = {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        query = session.query(models.Node.status, func.count(
            models.Node.id)).group_by(models.Node.status).filter_by(deleted=0)
        query = process_filters(models.Node)(query, filters)
    return query.all()


@require_context
def pool_status_get(context):
    session = get_session()
    filters = {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        query = session.query(models.Pool.status, func.count(
            models.Pool.id)).group_by(models.Pool.status).filter_by(deleted=0)
        query = process_filters(models.Pool)(query, filters)
    return query.all()


@require_context
def osd_status_get(context):
    session = get_session()
    filters = {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        query = session.query(models.Osd.status, func.count(
            models.Osd.id)).group_by(models.Osd.status).filter_by(deleted=0)
        query = process_filters(models.Osd)(query, filters)
    return query.all()


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def cluster_update(context, cluster_id, values):
    session = get_session()
    with session.begin():
        query = _cluster_get_query(context, session)
        result = query.filter_by(id=cluster_id).update(values)
        if not result:
            raise exception.ClusterNotFound(cluster_id=cluster_id)


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
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
def cluster_get_new_uuid(context):
    while True:
        uid = str(uuid.uuid4())
        return uid


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def cluster_create(context, values):
    if not values.get('id'):
        uid = cluster_get_new_uuid(context)
        values['id'] = uid
    else:
        uid = values.get('id')

    cluster_ref = models.Cluster()
    cluster_ref.update(values)
    session = get_session()
    with session.begin():
        session.add(cluster_ref)

    return _cluster_get(context, values['id'], session=session)


def cluster_destroy(context, cluster_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Cluster, session=session). \
            filter_by(id=cluster_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


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
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def rpc_service_create(context, values):
    rpc_service_ref = models.RPCService()
    rpc_service_ref.update(values)
    session = get_session()
    with session.begin():
        rpc_service_ref.save(session)

    return rpc_service_ref


def rpc_service_destroy(context, rpc_service_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.RPCService, session=session). \
            filter_by(id=rpc_service_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def rpc_service_get(context, rpc_service_id, expected_attrs=None):
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
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def rpc_service_update(context, rpc_service_id, values):
    session = get_session()
    with session.begin():
        query = _rpc_service_get_query(context, session)
        result = query.filter_by(id=rpc_service_id).update(values)
        if not result:
            raise exception.RPCServiceNotFound(rpc_service_id=rpc_service_id)


###############################


def _node_get_query(context, session=None):
    return model_query(
        context, models.Node, session=session
    )


def _node_get(context, node_id, session=None):
    result = _node_get_query(context, session)
    result = result.filter_by(id=node_id).first()

    if not result:
        raise exception.NodeNotFound(node_id=node_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def node_create(context, values):
    node_ref = models.Node()
    node_ref.update(values)
    node_ref.cluster_id = context.cluster_id
    session = get_session()
    with session.begin():
        node_ref.save(session)

    return node_ref


def node_destroy(context, node_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Node, session=session). \
            filter_by(id=node_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _node_load_attr(node, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'disks' in expected_attrs:
        node.disks = [disk for disk in node._disks if disk.deleted is False]
    if 'networks' in expected_attrs:
        node.networks = [net for net in node._networks if net.deleted is False]
    if 'osds' in expected_attrs:
        node.osds = [osd for osd in node._osds if osd.deleted is False]
    if 'radosgws' in expected_attrs:
        node.radosgws = [rgw for rgw in node._radosgws if rgw.deleted is False]


@require_context
def node_get(context, node_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        node = _node_get(context, node_id, session)
        _node_load_attr(node, expected_attrs)
    return node


@require_context
def node_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None,
                 expected_attrs=None):
    filters = filters or {}
    if filters.get("cluster_id") != "*":
        if "cluster_id" not in filters.keys():
            filters['cluster_id'] = context.cluster_id
    else:
        filters.pop("cluster_id")
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
        nodes = query.all()

        if not expected_attrs:
            return nodes

        for node in nodes:
            _node_load_attr(node, expected_attrs)
        return nodes


@require_context
def node_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _node_get_query(context, session)
        query = process_filters(models.Node)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
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
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def datacenter_create(context, values):
    session = get_session()
    datacenter_ref = models.Datacenter()
    datacenter_ref.update(values)
    if "cluster_id" not in values:
        datacenter_ref.cluster_id = context.cluster_id
    with session.begin():
        datacenter_ref.save(session)

    return datacenter_ref


def datacenter_destroy(context, datacenter_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Datacenter, session=session). \
            filter_by(id=datacenter_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def datacenter_get(context, datacenter_id, expected_attrs=None):
    return _datacenter_get(context, datacenter_id)


@require_context
def datacenter_get_all(context, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
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
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
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
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def rack_create(context, values):
    session = get_session()
    rack_ref = models.Rack()
    rack_ref.update(values)
    if "cluster_id" not in values:
        rack_ref.cluster_id = context.cluster_id
    with session.begin():
        rack_ref.save(session)

    return rack_ref


def rack_destroy(context, rack_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Rack, session=session). \
            filter_by(id=rack_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _rack_load_attr(rack, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'nodes' in expected_attrs:
        rack.nodes = [node for node in rack._nodes if not node.deleted]


@require_context
def rack_get(context, rack_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        rack = _rack_get(context, rack_id, session)
        _rack_load_attr(rack, expected_attrs)
    return rack


@require_context
def rack_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
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
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def rack_update(context, rack_id, values):
    session = get_session()
    with session.begin():
        query = _rack_get_query(context, session)
        result = query.filter_by(id=rack_id).update(values)
        if not result:
            raise exception.RackNotFound(rack_id=rack_id)


###############################


def _osd_get_query(context, session=None, **kwargs):
    return model_query(context, models.Osd, session=session, **kwargs)


def _osd_get(context, osd_id, session=None, **kwargs):
    result = _osd_get_query(context, session)
    result = result.filter_by(id=osd_id).first()

    if not result:
        raise exception.OsdNotFound(osd_id=osd_id)

    return result


def _osd_load_attr(osd, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'node' in expected_attrs:
        osd.node = osd._node
    if 'disk' in expected_attrs:
        osd.disk = osd._disk
    if 'pools' in expected_attrs:
        crush = osd._crush_rule
        if crush:
            osd.pools = [pool for pool in crush._pools if not pool.deleted]
        else:
            osd.pools = []
    if 'cache_partition' in expected_attrs:
        osd.cache_partition = osd._cache_partition
    if 'db_partition' in expected_attrs:
        osd.db_partition = osd._db_partition
    if 'wal_partition' in expected_attrs:
        osd.wal_partition = osd._wal_partition
    if 'journal_partition' in expected_attrs:
        osd.journal_partition = osd._journal_partition


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def osd_create(context, values):
    osd_ref = models.Osd()
    osd_ref.cluster_id = context.cluster_id
    osd_ref.update(values)
    session = get_session()
    with session.begin():
        osd_ref.save(session)

    return osd_ref


def osd_destroy(context, osd_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Osd, session=session). \
            filter_by(id=osd_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def osd_get(context, osd_id, **kwargs):
    expected_attrs = kwargs.pop("expected_attrs", None)
    session = get_session()
    with session.begin():
        osd = _osd_get(context, osd_id, session, **kwargs)
        _osd_load_attr(osd, expected_attrs)
        return osd


@require_context
def osd_get_by_osd_id(context, osd_id, **kwargs):
    expected_attrs = kwargs.pop("expected_attrs", None)
    session = get_session()
    with session.begin():
        osd = _osd_get_query(
            context, session, **kwargs
        ).filter_by(
            osd_id=osd_id, cluster_id=context.cluster_id
        ).first()
        if not osd:
            raise exception.OsdNotFound(osd_id=osd_id)
        if not expected_attrs:
            return osd
        _osd_load_attr(osd, expected_attrs)
        return osd


@require_context
def osd_get_all(context, marker=None, limit=None, sort_keys=None,
                sort_dirs=None, filters=None, offset=None,
                expected_attrs=None):
    session = get_session()
    filters = filters or {}
    if filters.get("cluster_id") != "*":
        if "cluster_id" not in filters.keys():
            filters['cluster_id'] = context.cluster_id
    else:
        filters.pop("cluster_id")
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Osd, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        osds = query.all()
        if not expected_attrs:
            return osds
        for osd in osds:
            _osd_load_attr(osd, expected_attrs)
        return osds


@require_context
def osd_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _osd_get_query(context, session)
        query = process_filters(models.Osd)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def osd_update(context, osd_id, values):
    session = get_session()
    with session.begin():
        query = _osd_get_query(context, session)
        result = query.filter_by(id=osd_id).update(values)
        if not result:
            raise exception.OsdNotFound(osd_id=osd_id)


###############################


def _pool_get_query(context, session=None):
    return model_query(context, models.Pool, session=session)


def _pool_get(context, pool_id, session=None):
    result = _pool_get_query(context, session)
    result = result.filter_by(id=pool_id).first()

    if not result:
        raise exception.PoolNotFound(pool_id=pool_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def pool_create(context, values):
    pool_ref = models.Pool()
    pool_ref.update(values)
    if "cluster_id" not in values.keys():
        pool_ref.cluster_id = context.cluster_id
    session = get_session()
    with session.begin():
        pool_ref.save(session)

    return pool_ref


def pool_destroy(context, pool_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Pool, session=session). \
            filter_by(id=pool_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _pool_load_attr(ctxt, pool, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'crush_rule' in expected_attrs:
        pool.crush_rule = pool._crush_rule
    if 'osds' in expected_attrs:
        if not pool.crush_rule:
            pool.osds = []
        else:
            pool.osds = [osd for osd in pool.crush_rule._osds
                         if not osd.deleted]
    if 'volumes' in expected_attrs:
        filters = {"pool_id": pool.id}
        volumes = volume_get_all(ctxt, filters=filters)
        pool.volumes = [volume for volume in volumes if not volume.deleted]
    if 'policies' in expected_attrs:
        if pool.role == 'index':
            pool.policies = [policy for policy in pool._index_policies if
                             not policy.deleted]
        elif pool.role == 'data':
            pool.policies = [policy for policy in pool._data_policies if
                             not policy.deleted]
        else:
            pool.policies = []


@require_context
def pool_get(context, pool_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        pool = _pool_get(context, pool_id, session)
        _pool_load_attr(context, pool, expected_attrs)
        return pool


def pool_filters(query, filters):
    filters = filters.copy()
    crush_rule_type = None
    if 'failure_domain_type' in filters.keys():
        crush_rule_type = filters.pop('failure_domain_type')
    query = process_filters(models.Pool)(query, filters)
    if crush_rule_type:
        query = query.outerjoin(models.CrushRule).filter_by(
            type=crush_rule_type)
    return query


@require_context
def pool_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None,
                 expected_attrs=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Pool, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        pools = query.all()
        if not expected_attrs:
            return pools
        for pool in pools:
            _pool_load_attr(context, pool, expected_attrs)
        return pools


@require_context
def pool_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _pool_get_query(context, session)
        query = pool_filters(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def pool_update(context, pool_id, values):
    session = get_session()
    with session.begin():
        query = _pool_get_query(context, session)
        result = query.filter_by(id=pool_id).update(values)
        if not result:
            raise exception.PoolNotFound(pool_id=pool_id)


@require_context
def osd_get_by_pool(context, pool_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        pool = _pool_get(context, pool_id, session)  # model object
        crush = pool._crush_rule
        osds = crush._osds
        if not expected_attrs:
            return osds
        for osd in osds:
            _osd_load_attr(osd, expected_attrs)
        return osds


###############################


def _sys_config_get_query(context, session=None):
    # TODO  filter_by cluster id
    return model_query(context, models.SysConfig, session=session)


def _sys_config_get(context, sys_config_id, session=None):
    result = _sys_config_get_query(context, session)
    result = result.filter_by(id=sys_config_id).first()

    if not result:
        raise exception.SysConfigNotFound(sys_config_id=sys_config_id)

    return result


def sys_config_get_by_key(context, key, cluster_id, session=None):
    result = _sys_config_get_query(
        context,
        session).filter_by(key=key, cluster_id=cluster_id).first()

    if not result:
        return None

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def sys_config_create(context, values):
    # TODO
    # get cluster id from context
    sys_config_ref = models.SysConfig()
    sys_config_ref.update(values)
    if "cluster_id" not in values.keys():
        sys_config_ref.cluster_id = context.cluster_id
    session = get_session()
    with session.begin():
        sys_config_ref.save(session)

    return sys_config_ref


def sys_config_destroy(context, sys_config_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.SysConfig, session=session). \
            filter_by(id=sys_config_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def sys_config_get(context, sys_config_id, expected_attrs=None):
    return _sys_config_get(context, sys_config_id)


@require_context
def sys_config_get_all(context, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.SysConfig, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def sys_config_update(context, sys_config_id, values):
    session = get_session()
    with session.begin():
        query = _sys_config_get_query(context, session)
        result = query.filter_by(id=sys_config_id).update(values)
        if not result:
            raise exception.SysConfigNotFound(sys_config_id=sys_config_id)


###############################

@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_access_path_create(context, values):
    volume_access_path_ref = models.VolumeAccessPath()
    volume_access_path_ref.update(values)

    session = get_session()
    with session.begin():
        volume_access_path_ref.save(session)

    return volume_access_path_ref


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_access_path_destroy(context, access_path_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeAccessPath, session=session). \
            filter_by(id=access_path_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def _volume_access_path_get_query(context, session=None):
    """Get the query to retrieve the volume.

    :param context: the context used to run _volume_access_path_get_query
    :param session: the session to use
    :returns: updated query or None
    """
    return model_query(context, models.VolumeAccessPath, session=session)


@require_context
def _volume_access_path_get(context, access_path_id, session=None,
                            joined_load=True):
    result = _volume_access_path_get_query(
        context, session=session)
    result = result.filter_by(id=access_path_id).first()

    if not result:
        raise exception.VolumeAccessPathNotFound(access_path_id=access_path_id)

    return result


def _volume_access_path_load_attr(ctxt, vap, session, expected_attrs=None):
    expected_attrs = expected_attrs or []
    cg_ids = []
    vol_ids = []
    mappings = _volume_mapping_get_query(
        ctxt, session).filter_by(volume_access_path_id=vap.id)
    for mapping in mappings:
        cg_id = mapping.volume_client_group_id
        vol_id = mapping.volume_id
        if cg_id not in cg_ids:
            cg_ids.append(cg_id)
        if vol_id and vol_id not in vol_ids:
            vol_ids.append(vol_id)
    if 'volume_gateways' in expected_attrs:
        vap.volume_gateways = [vgw for vgw in vap._volume_gateways
                               if not vgw.deleted]
    if 'volume_client_groups' in expected_attrs:
        column_attr = getattr(models.VolumeClientGroup, "id")
        cgs = _volume_client_group_get_query(
            ctxt, session).filter(column_attr.in_(cg_ids))
        vap.volume_client_groups = [cg for cg in cgs]
    if 'nodes' in expected_attrs and vap.volume_gateways:
        vap.nodes = []
        for vg in vap.volume_gateways:
            vap.nodes.append(_node_get(ctxt, vg.node_id, session))
    if 'volumes' in expected_attrs and vap.volume_client_groups:
        column_attr = getattr(models.Volume, "id")
        vols = _volume_get_query(ctxt, session).filter(
            column_attr.in_(vol_ids))
        vap.volumes = [vol for vol in vols]
    if 'volume_clients' in expected_attrs and vap.volume_client_groups:
        vap.volume_clients = []
        for vcg in vap.volume_client_groups:
            volume_clients = _volume_client_get_query(ctxt, session).filter_by(
                volume_client_group_id=vcg.id)
            vap.volume_clients.extend(volume_clients)


@require_context
def volume_access_path_get(context, access_path_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        vap = _volume_access_path_get(context, access_path_id, session)
        _volume_access_path_load_attr(
            context, vap, session, expected_attrs=expected_attrs)
        return vap


@require_context
def volume_access_path_get_all(context, marker=None,
                               limit=None, sort_keys=None,
                               sort_dirs=None, filters=None, offset=None,
                               expected_attrs=None):
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
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.VolumeAccessPath,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No volumes would match, return empty list
        if query is None:
            return []
        vaps = query.all()
        if not expected_attrs:
            return vaps
        for vap in vaps:
            _volume_access_path_load_attr(context, vap, session,
                                          expected_attrs=expected_attrs)
        return vaps


@require_context
def volume_access_path_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _volume_access_path_get_query(context, session)
        query = process_filters(models.VolumeAccessPath)(query, filters)
        return query.count()


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_access_path_update(context, access_path_id, values):
    session = get_session()
    with session.begin():
        query = _volume_access_path_get_query(
            context, session)
        result = query.filter_by(id=access_path_id).update(values)
        if not result:
            raise exception.VolumeAccessPathNotFound(
                access_path_id=access_path_id)


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_access_paths_update(context, values_list):
    session = get_session()
    with session.begin():
        volume_access_path_refs = []
        for values in values_list:
            access_path_id = values['id']
            values.pop('id')
            volume_access_path_ref = _volume_access_path_get(
                context,
                access_path_id,
                session=session)
            volume_access_path_ref.update(values)
            volume_access_path_refs.append(volume_access_path_ref)

        return volume_access_path_refs


@handle_db_data_error
@require_context
def volume_access_path_append_gateway(context, access_path_id,
                                      volume_gateway_id):
    session = get_session()
    with session.begin():
        volume_access_path_ref = _volume_access_path_get(
            context, access_path_id, session=session)
        volume_gateway_ref = _volume_gateway_get(
            context, volume_gateway_id, session=session)
        volume_access_path_ref._volume_gateways.append(volume_gateway_ref)
        volume_access_path_ref.updated_at = timeutils.utcnow()
        volume_access_path_ref.save(session)
    return volume_access_path_ref


@handle_db_data_error
@require_context
def volume_access_path_remove_gateway(context, access_path_id,
                                      volume_gateway_id):
    session = get_session()
    with session.begin():
        volume_access_path_ref = _volume_access_path_get(
            context, access_path_id, session=session)
        volume_gateway_ref = _volume_gateway_get(
            context, volume_gateway_id, session=session)
        volume_access_path_ref._volume_gateways.remove(volume_gateway_ref)
        volume_access_path_ref.updated_at = timeutils.utcnow()
        volume_access_path_ref.save(session)
    return volume_access_path_ref


###############################


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_gateway_create(context, values):
    volume_gateway_ref = models.VolumeGateway()
    volume_gateway_ref.update(values)

    session = get_session()
    with session.begin():
        volume_gateway_ref.save(session)

    return volume_gateway_ref


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_gateway_destroy(context, ap_gateway_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeGateway, session=session). \
            filter_by(id=ap_gateway_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def _volume_gateway_get_query(context, session=None):
    """Get the query to retrieve the volume.

    :param context: the context used to run _volume_gateway_get_query
    :param session: the session to use
    :returns: updated query or None
    """
    return model_query(context, models.VolumeGateway, session=session)


@require_context
def _volume_gateway_get(context, ap_gateway_id, session=None):
    result = _volume_gateway_get_query(
        context, session=session)
    result = result.filter_by(id=ap_gateway_id).first()

    if not result:
        raise exception.VolumeGatewayNotFound(gateway_id=ap_gateway_id)

    return result


@require_context
def volume_gateway_get(context, ap_gateway_id, expected_attrs=None):
    return _volume_gateway_get(context, ap_gateway_id)


@require_context
def volume_gateway_get_all(context, marker=None,
                           limit=None, sort_keys=None,
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
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.VolumeGateway,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No volumes would match, return empty list
        if query is None:
            return []
        return query.all()


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_gateway_update(context, ap_gateway_id, values):
    session = get_session()
    with session.begin():
        query = _volume_gateway_get_query(
            context, session, joined_load=False)
        result = query.filter_by(id=ap_gateway_id).update(values)
        if not result:
            raise exception.VolumeGatewayNotFound(
                ap_gateway_id=ap_gateway_id)


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_gateways_update(context, values_list):
    session = get_session()
    with session.begin():
        volume_gateways_ref = []
        for values in values_list:
            ap_gateway_id = values['id']
            values.pop('id')
            volume_gateway_ref = _volume_gateway_get(
                context,
                ap_gateway_id,
                session=session)
            volume_gateway_ref.update(values)
            volume_gateways_ref.append(volume_gateway_ref)

        return volume_gateways_ref


@require_context
def volume_gateway_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _volume_gateway_get_query(context, session)
        query = process_filters(models.Radosgw)(query, filters)
        return query.count()


###############################
@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_mapping_create(context, values):
    volume_mapping_ref = models.VolumeMapping()
    volume_mapping_ref.update(values)

    session = get_session()
    with session.begin():
        volume_mapping_ref.save(session)

    return volume_mapping_ref


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_mapping_destroy(context, volume_mapping_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeMapping, session=session). \
            filter_by(id=volume_mapping_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def _volume_mapping_get_query(context, session=None):
    """Get the query to retrieve the volume.

    :param context: the context used to run _volume_mapping_get_query
    :param session: the session to use
    :returns: updated query or None
    """
    return model_query(context, models.VolumeMapping, session=session)


@require_context
def _volume_mapping_get(context, volume_mapping_id, session=None):
    result = _volume_mapping_get_query(
        context, session=session)
    result = result.filter_by(id=volume_mapping_id).first()

    if not result:
        raise exception.VolumeMappingNotFound(
            volume_mapping_id=volume_mapping_id)

    return result


@require_context
def volume_mapping_get(context, volume_mapping_id, expected_attrs=None):
    return _volume_mapping_get(context, volume_mapping_id)


@require_context
def volume_mapping_get_all(context, marker=None,
                           limit=None, sort_keys=None,
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
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.VolumeMapping,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No volumes would match, return empty list
        if query is None:
            return []
        return query.all()


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_mapping_update(context, volume_mapping_id, values):
    session = get_session()
    with session.begin():
        query = _volume_mapping_get_query(
            context, session)
        result = query.filter_by(id=volume_mapping_id).update(values)
        if not result:
            raise exception.VolumeMappingNotFound(
                volume_mapping_id=volume_mapping_id)


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_mappings_update(context, values_list):
    session = get_session()
    with session.begin():
        volume_mappings_ref = []
        for values in values_list:
            volume_mapping_id = values['id']
            values.pop('id')
            volume_mapping_ref = _volume_mapping_get(
                context,
                volume_mapping_id,
                session=session)
            volume_mapping_ref.update(values)
            volume_mappings_ref.append(volume_mapping_ref)

        return volume_mappings_ref


###############################


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_client_create(context, values):
    volume_client_ref = models.VolumeClient()
    volume_client_ref.update(values)

    session = get_session()
    with session.begin():
        volume_client_ref.save(session)

    return volume_client_ref


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_client_destroy(context, volume_client_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeClient, session=session). \
            filter_by(id=volume_client_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def _volume_client_get_query(context, session=None):
    """Get the query to retrieve the volume.

    :param context: the context used to run _volume_client_get_query
    :param session: the session to use
    :returns: updated query or None
    """
    return model_query(context, models.VolumeClient, session=session)


@require_context
def _volume_client_get(context, volume_client_id, session=None):
    result = _volume_client_get_query(
        context, session=session)
    result = result.filter_by(id=volume_client_id).first()

    if not result:
        raise exception.VolumeClientNotFound(volume_client_id=volume_client_id)

    return result


@require_context
def volume_client_get(context, volume_client_id, expected_attrs=None):
    return _volume_client_get(context, volume_client_id)


@require_context
def volume_client_get_all(context, marker=None,
                          limit=None, sort_keys=None,
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
            context, session, models.VolumeClient,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No volumes would match, return empty list
        if query is None:
            return []
        return query.all()


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_client_update(context, volume_client_id, values):
    session = get_session()
    with session.begin():
        query = _volume_client_get_query(
            context, session, joined_load=False)
        result = query.filter_by(id=volume_client_id).update(values)
        if not result:
            raise exception.VolumeClientNotFound(
                volume_client_id=volume_client_id)


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_clients_update(context, values_list):
    session = get_session()
    with session.begin():
        volume_clients_ref = []
        for values in values_list:
            volume_client_id = values['id']
            values.pop('id')
            volume_client_ref = _volume_client_get(
                context,
                volume_client_id,
                session=session)
            volume_client_ref.update(values)
            volume_clients_ref.append(volume_client_ref)

        return volume_clients_ref


###############################


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_client_group_create(context, values):
    volume_client_group_ref = models.VolumeClientGroup()
    volume_client_group_ref.update(values)

    session = get_session()
    with session.begin():
        volume_client_group_ref.save(session)

    return volume_client_group_ref


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_client_group_destroy(context, client_group_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeClientGroup, session=session). \
            filter_by(id=client_group_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def _volume_client_group_get_query(context, session=None):
    """Get the query to retrieve the volume.

    :param context: the context used to run _volume_client_group_get_query
    :param session: the session to use
    :returns: updated query or None
    """
    return model_query(context, models.VolumeClientGroup, session=session)


@require_context
def _volume_client_group_get(context, client_group_id, session=None,
                             joined_load=True):
    result = _volume_client_group_get_query(
        context, session=session)
    result = result.filter_by(id=client_group_id).first()

    if not result:
        raise exception.VolumeClientGroupNotFound(
            client_group_id=client_group_id)

    return result


def _volume_client_group_load_attr(ctxt, vcg, session, expected_attrs=None):
    expected_attrs = expected_attrs or []
    vap_ids = []
    vol_ids = []
    mappings = _volume_mapping_get_query(
        ctxt, session).filter_by(volume_client_group_id=vcg.id)
    for mapping in mappings:
        vap_id = mapping.volume_access_path_id
        vol_id = mapping.volume_id
        if vap_id not in vap_ids:
            vap_ids.append(vap_id)
        if vol_id not in vol_ids:
            vol_ids.append(vol_id)
    if 'volume_access_paths' in expected_attrs:
        column_attr = getattr(models.VolumeAccessPath, "id")
        vaps = _volume_access_path_get_query(
            ctxt, session).filter(column_attr.in_(vap_ids))
        if vaps:
            vcg.volume_access_paths = [vap for vap in vaps if not vap.deleted]
        else:
            vcg.volume_access_paths = None
    if 'volume_clients' in expected_attrs:
        volume_clients = _volume_client_get_query(ctxt, session).filter_by(
            volume_client_group_id=vcg.id)
        vcg.volume_clients = [client for client in volume_clients]
    if 'volumes' in expected_attrs:
        column_attr = getattr(models.Volume, "id")
        vols = _volume_get_query(ctxt, session).filter(
            column_attr.in_(vol_ids))
        vcg.volumes = [vol for vol in vols]


@require_context
def volume_client_group_get(context, client_group_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        vcg = _volume_client_group_get(
            context, client_group_id, session)
        _volume_client_group_load_attr(context, vcg, session, expected_attrs)
        return vcg


@require_context
def volume_client_group_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _volume_client_group_get_query(context, session)
        query = process_filters(models.VolumeClientGroup)(query, filters)
        return query.count()


@require_context
def volume_client_group_get_all(context, marker=None,
                                limit=None, sort_keys=None,
                                sort_dirs=None, filters=None, offset=None,
                                expected_attrs=None):
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
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.VolumeClientGroup,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No volumes would match, return empty list
        if query is None:
            return []
        vcgs = query.all()
        if not expected_attrs:
            return vcgs
        for vcg in vcgs:
            _volume_client_group_load_attr(context, vcg, session,
                                           expected_attrs)
        return vcgs


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_client_group_update(context, client_group_id, values):
    session = get_session()
    with session.begin():
        query = _volume_client_group_get_query(
            context, session)
        result = query.filter_by(id=client_group_id).update(values)
        if not result:
            raise exception.VolumeClientGroupNotFound(
                client_group_id=client_group_id)


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_client_groups_update(context, values_list):
    session = get_session()
    with session.begin():
        volume_client_groups_ref = []
        for values in values_list:
            client_group_id = values['id']
            values.pop('id')
            volume_client_group_ref = _volume_client_group_get(
                context,
                client_group_id,
                session=session)
            volume_client_group_ref.update(values)
            volume_client_groups_ref.append(volume_client_group_ref)

        return volume_client_groups_ref


def volume_client_get_by_iqn(context, iqn):
    session = get_session()
    with session.begin():
        result = _volume_client_get_query(context, session)
        result = result.filter_by(iqn=iqn).first()

        if not result:
            return None
        return result
###############################


@require_context
def _license_get_query(context, session=None):
    return model_query(context, models.LicenseFile, session=session)


@require_context
def _license_get(context, license_id, session=None):
    result = _license_get_query(context, session=session)
    result = result.filter_by(id=license_id).first()

    if not result:
        raise exception.LicenseNotFound(license_id=license_id)

    return result


def _process_license_filters(query, filters):
    filters = filters.copy()
    for key in filters.keys():
        try:
            column_attr = getattr(models.LicenseFile, key)
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
            column_attr = getattr(models.LicenseFile, key)
            query = query.filter(column_attr.in_(value))
        else:
            # OK, simple exact match; save for later
            filter_dict[key] = value

    # Apply simple exact matches
    if filter_dict:
        query = query.filter_by(**filter_dict)
    return query


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def license_create(context, values):
    license_ref = models.LicenseFile()
    license_ref.update(values)
    session = get_session()
    with session.begin():
        license_ref.save(session)

    return license_ref


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def license_update(context, license_id, values):
    session = get_session()
    with session.begin():
        query = _license_get_query(context, session)
        result = query.filter_by(id=license_id).update(values)
        if not result:
            raise exception.LicenseNotFound(license_id=license_id)


@require_context
def license_get_all(context, marker=None, limit=None, sort_keys=None,
                    sort_dirs=None, filters=None, offset=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.LicenseFile,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


###############################


def _network_get_query(context, session=None):
    return model_query(context, models.Network, session=session)


def _network_get(context, net_id, session=None):
    result = _network_get_query(context, session)
    result = result.filter_by(id=net_id).first()

    if not result:
        raise exception.NetworkNotFound(net_id=net_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def network_create(context, values):
    net_ref = models.Network()
    net_ref.cluster_id = context.cluster_id
    net_ref.update(values)
    session = get_session()
    with session.begin():
        net_ref.save(session)
    return net_ref


@require_context
def network_destroy(context, net_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Network, session=session). \
            filter_by(id=net_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def network_get(context, net_id, expected_attrs=None):
    return _network_get(context, net_id)


@require_context
def network_get_all(context, marker=None, limit=None, sort_keys=None,
                    sort_dirs=None, filters=None, offset=None,
                    expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Network, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        networks = query.all()
        expected_attrs = expected_attrs or []
        if 'node' in expected_attrs:
            for net in networks:
                net.node = net._node
        return networks


@require_context
def network_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _network_get_query(context, session)
        query = process_filters(models.Network)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def network_update(context, net_id, values):
    session = get_session()
    with session.begin():
        query = _network_get_query(context, session)
        result = query.filter_by(id=net_id).update(values)
        if not result:
            raise exception.NetworkNotFound(net_id=net_id)


###############################


@require_context
def _alert_rule_get_query(context, session=None):
    return model_query(context, models.AlertRule, session=session)


def _alert_rule_get(context, alert_rule_id, session=None):
    result = _alert_rule_get_query(context, session=session)
    result = result.filter_by(id=alert_rule_id).first()

    if not result:
        raise exception.AlertRuleNotFound(alert_rule_id=alert_rule_id)

    return result


def _alert_rule_load_attr(ctxt, alert_rule, expected_attrs, session=None):
    expected_attrs = expected_attrs or []
    if 'alert_groups' in expected_attrs:
        alert_rule.alert_groups = [
            al_group for al_group in alert_rule.alert_groups
            if not al_group.deleted]


@require_context
def alert_rule_get(context, alert_rule_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        alert_rule = _alert_rule_get(context, alert_rule_id, session)
        _alert_rule_load_attr(context, alert_rule, expected_attrs, session)
    return alert_rule


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def alert_rule_create(context, values):
    alert_rule_ref = models.AlertRule()
    alert_rule_ref.update(values)
    session = get_session()
    with session.begin():
        alert_rule_ref.save(session)

    return alert_rule_ref


def alert_rule_destroy(context, alert_rule_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.AlertRule, session=session). \
            filter_by(id=alert_rule_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def alert_rule_update(context, alert_rule_id, values):
    session = get_session()
    with session.begin():
        query = _alert_rule_get_query(context, session)
        result = query.filter_by(id=alert_rule_id).update(values)
        if not result:
            raise exception.AlertRuleNotFound(alert_rule_id=alert_rule_id)


@require_context
def alert_rule_get_all(context, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
    if filters.get('cluster_id') != "*":
        if "cluster_id" not in filters.keys():
            filters['cluster_id'] = context.cluster_id
    else:
        filters.pop('cluster_id')
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.AlertRule,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


@require_context
def alert_rule_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _alert_rule_get_query(context, session)
        query = process_filters(models.AlertRule)(query, filters)
        return query.count()


###############################


def _disk_load_attr(context, disk, expected_attrs=None, session=None):
    expected_attrs = expected_attrs or []
    if 'node' in expected_attrs:
        disk.node = disk._node
    if "partition_used" in expected_attrs:
        parts = model_query(
            context, models.DiskPartition, session=session
        ).filter_by(status='inuse', disk_id=disk.id)
        disk.partition_used = parts.count()
    if "partitions" in expected_attrs:
        disk.partitions = [part for part in disk._partitions
                           if not part.deleted]


def _disk_get_query(context, session=None):
    return model_query(
        context, models.Disk, session=session
    )


def _disk_get(context, disk_id, session=None):
    result = _disk_get_query(context, session)
    result = result.filter_by(id=disk_id).first()

    if not result:
        raise exception.DiskNotFound(disk_id=disk_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def disk_create(context, values):
    disk_ref = models.Disk()
    disk_ref.cluster_id = context.cluster_id
    disk_ref.update(values)
    session = get_session()
    with session.begin():
        disk_ref.save(session)

    return disk_ref


def disk_destroy(context, disk_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Disk, session=session). \
            filter_by(id=disk_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def disk_get(context, disk_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        disk = _disk_get(context, disk_id, session)
        _disk_load_attr(context, disk, expected_attrs, session)
    return disk


@require_context
def disk_get_by_slot(context, slot, node_id, **kwargs):
    expected_attrs = kwargs.pop("expected_attrs", None)
    session = get_session()
    with session.begin():
        disk = _disk_get_query(
            context, session,
        ).filter_by(slot=slot, node_id=node_id).first()
        if not disk:
            return None
        _disk_load_attr(context, disk, expected_attrs, session)
        return disk


@require_context
def disk_get_by_name(context, name, node_id, **kwargs):
    expected_attrs = kwargs.pop("expected_attrs", None)
    session = get_session()
    with session.begin():
        disk = _disk_get_query(
            context, session,
        ).filter_by(name=name, node_id=node_id).first()
        if not disk:
            return None
        _disk_load_attr(context, disk, expected_attrs, session)
        return disk


@require_context
def disk_get_by_guid(context, guid, node_id, **kwargs):
    expected_attrs = kwargs.pop("expected_attrs", None)
    session = get_session()
    with session.begin():
        disk = _disk_get_query(
            context, session,
        ).filter_by(guid=guid, node_id=node_id).first()
        if not disk:
            return None
        _disk_load_attr(context, disk, expected_attrs, session)
        return disk


@require_context
def disk_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None,
                 expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Disk, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        disks = query.all()
        if not expected_attrs:
            return disks
        for disk in disks:
            _disk_load_attr(context, disk, expected_attrs, session)
        return disks


@require_context
def disk_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _disk_get_query(context, session)
        query = process_filters(models.Disk)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def disk_update(context, disk_id, values):
    session = get_session()
    with session.begin():
        query = _disk_get_query(context, session)
        result = query.filter_by(id=disk_id).update(values)
        if not result:
            raise exception.DiskNotFound(disk_id=disk_id)


@require_context
def disk_get_all_available(context, filters=None, expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        query = _disk_get_query(context, session)
        query = process_filters(models.Disk)(query, filters)
        query = query.outerjoin(models.Node).filter_by(role_storage=True)
        if query is None:
            return []
        disks = query.all()
        if not expected_attrs:
            return disks
        for disk in disks:
            _disk_load_attr(context, disk, expected_attrs, session)
        return disks


###############################


def _disk_partition_load_attr(context,
                              disk_partition,
                              expected_attrs=None,
                              session=None):
    expected_attrs = expected_attrs or []
    if 'node' in expected_attrs:
        disk_partition.node = disk_partition._node
    if "disk" in expected_attrs:
        disk_partition.disk = disk_partition._disk


def _disk_partition_get_query(context, session=None):
    return model_query(
        context, models.DiskPartition, session=session
    ).filter_by(cluster_id=context.cluster_id)


def _disk_partition_get(context, disk_part_id, session=None):
    result = _disk_partition_get_query(context, session)
    result = result.filter_by(id=disk_part_id).first()

    if not result:
        raise exception.DiskPartitionNotFound(disk_part_id=disk_part_id)

    return result


@require_context
def disk_partition_get_by_uuid(context, uuid, node_id, **kwargs):
    expected_attrs = kwargs.pop("expected_attrs", None)
    session = get_session()
    with session.begin():
        disk_partition = _disk_partition_get_query(
            context, session,
        ).filter_by(uuid=uuid, node_id=node_id).first()
        if not disk_partition:
            return None
        _disk_partition_load_attr(
            context, disk_partition, expected_attrs, session)
        return disk_partition


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def disk_partition_create(context, values):
    disk_part_ref = models.DiskPartition()
    disk_part_ref.cluster_id = context.cluster_id
    disk_part_ref.update(values)
    session = get_session()
    with session.begin():
        disk_part_ref.save(session)

    return disk_part_ref


def disk_partition_destroy(context, disk_part_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.DiskPartition, session=session). \
            filter_by(id=disk_part_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _disk_pattition_load_attr(partition, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'disk' in expected_attrs:
        partition.disk = partition._disk
    if 'node' in expected_attrs:
        partition.node = partition._disk._node


@require_context
def disk_partition_get(context, disk_part_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        partition = _disk_partition_get(context, disk_part_id)
        _disk_pattition_load_attr(partition, expected_attrs)
        return partition


@require_context
def disk_partition_get_all(context, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None,
                           expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.DiskPartition, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        partitions = query.all()
        if not expected_attrs:
            return partitions
        for partition in partitions:
            _disk_pattition_load_attr(partition, expected_attrs)
        return partitions


@require_context
def disk_partition_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _disk_partition_get_query(context, session)
        query = process_filters(models.DiskPartition)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def disk_partition_update(context, disk_part_id, values):
    session = get_session()
    with session.begin():
        query = _disk_partition_get_query(context, session)
        result = query.filter_by(id=disk_part_id).update(values)
        if not result:
            raise exception.DiskPartitionNotFound(disk_part_id=disk_part_id)


@require_context
def disk_partition_get_all_available(context, filters=None,
                                     expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        query = _disk_partition_get_query(context, session)
        query = process_filters(models.DiskPartition)(query, filters)
        query = query.outerjoin(models.Node).filter_by(role_storage=True)
        if query is None:
            return []
        partitions = query.all()
        if not expected_attrs:
            return partitions
        for partition in partitions:
            _disk_pattition_load_attr(partition, expected_attrs)
        return partitions


###############################


@require_context
def _alert_group_get_query(context, session=None):
    return model_query(context, models.AlertGroup, session=session)


def _alert_group_get(context, alert_group_id, session=None):
    result = _alert_group_get_query(context, session=session)
    result = result.filter_by(id=alert_group_id).first()

    if not result:
        raise exception.AlertGroupNotFound(alert_group_id=alert_group_id)

    return result


@require_context
def alert_group_get(context, alert_group_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        alert_group = _alert_group_get(context, alert_group_id, session)
        _alert_group_load_attr(context, alert_group, expected_attrs, session)
    return alert_group


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def alert_group_create(context, values):
    alert_group_ref = models.AlertGroup()
    alert_group_ref.cluster_id = context.cluster_id
    alert_rule_ids = values.pop('alert_rule_ids')
    db_rules = [alert_rule_get(context, rule_id) for rule_id in alert_rule_ids]
    email_group_ids = values.pop('email_group_ids')
    db_emails = [email_group_get(context, email_id)
                 for email_id in email_group_ids]
    # add relations:alert_rules,email_groups
    alert_group_ref.alert_rules = db_rules
    alert_group_ref.email_groups = db_emails
    alert_group_ref.update(values)
    session = get_session()
    with session.begin():
        alert_group_ref.save(session)
    return alert_group_ref


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def alert_group_update(context, alert_group_id, values):
    session = get_session()
    with session.begin():
        query = _alert_group_get_query(context, session)
        result = query.filter_by(id=alert_group_id).first()
        if not result:
            raise exception.AlertGroupNotFound(alert_group_id=alert_group_id)
        name = values.pop('name', None)
        if name:
            result.name = name
        # update relations:alert_rules,email_groups
        alert_rule_ids = values.pop('alert_rule_ids', None)
        if alert_rule_ids:
            db_rules = [
                _alert_rule_get(context, rule_id, session) for rule_id in
                alert_rule_ids
            ]
            result.alert_rules = db_rules
        email_group_ids = values.pop('email_group_ids', None)
        if email_group_ids:
            db_emails = [_email_group_get(context, email_id, session)
                         for email_id in email_group_ids]
            result.email_groups = db_emails
        result.save(session)


def _alert_group_load_attr(ctxt, ale_group, expected_attrs, session=None):
    expected_attrs = expected_attrs or []
    if 'alert_rules' in expected_attrs:
        ale_group.alert_rules = [al_rule for al_rule in ale_group.alert_rules
                                 if not al_rule.deleted]
    if 'email_groups' in expected_attrs:
        ale_group.email_groups = [
            al_email for al_email in ale_group.email_groups
            if not al_email.deleted
        ]


@require_context
def alert_group_get_all(context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None,
                        expected_attrs=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.AlertGroup,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        alert_groups = query.all()
        for ale_group in alert_groups:
            _alert_group_load_attr(context, ale_group, expected_attrs, session)
        return alert_groups


def alert_group_destroy(context, alert_group_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')
                          }
        db_obj = model_query(context, models.AlertGroup, session=session). \
            filter_by(id=alert_group_id).first()
        # del relations
        db_obj.alert_rules.clear()
        db_obj.email_groups.clear()
        db_obj.update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def alert_group_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _alert_group_get_query(context, session)
        query = process_filters(models.AlertGroup)(query, filters)
        return query.count()


###############################


@require_context
def _email_group_get_query(context, session=None):
    return model_query(context, models.EmailGroup, session=session)


def _email_group_get(context, email_group_id, session=None):
    result = _email_group_get_query(context, session=session)
    result = result.filter_by(id=email_group_id).first()

    if not result:
        raise exception.EmailGroupNotFound(email_group_id=email_group_id)

    return result


@require_context
def email_group_get(context, email_group_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        email_group = _email_group_get(context, email_group_id, session)
        _email_group_load_attr(context, email_group, expected_attrs, session)
    return email_group


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def email_group_create(context, values):
    email_group_ref = models.EmailGroup()
    email_group_ref.update(values)
    session = get_session()
    with session.begin():
        email_group_ref.save(session)

    return email_group_ref


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def email_group_update(context, email_group_id, values):
    session = get_session()
    with session.begin():
        query = _email_group_get_query(context, session)
        result = query.filter_by(id=email_group_id).update(values)
        if not result:
            raise exception.EmailGroupNotFound(email_group_id=email_group_id)


def _email_group_load_attr(context, email_group, expected_attrs, session):
    expected_attrs = expected_attrs or []
    if 'alert_groups' in expected_attrs:
        email_group.alert_groups = [al_group for al_group in
                                    email_group.alert_groups
                                    if not al_group.deleted]


@require_context
def email_group_get_all(context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None,
                        expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.EmailGroup,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
    email_groups = query.all()
    for email_group in email_groups:
        _email_group_load_attr(context, email_group, expected_attrs, session)
    return email_groups


@require_context
def email_group_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _email_group_get_query(context, session)
        query = process_filters(models.EmailGroup)(query, filters)
        return query.count()


def email_group_destroy(context, email_group_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        db_obj = model_query(context, models.EmailGroup, session=session). \
            filter_by(id=email_group_id).first()
        if db_obj.alert_groups:
            raise exception.EmailGroupDeleteError()
        else:
            db_obj.update(updated_values)
    del updated_values['updated_at']
    return updated_values


###############################


@require_context
def _alert_log_get_query(context, session=None):
    return model_query(context, models.AlertLog, session=session)


def _alert_log_get(context, alert_log_id, session=None):
    result = _alert_log_get_query(context, session=session)
    result = result.filter_by(id=alert_log_id).first()

    if not result:
        raise exception.AlertLogNotFound(alert_log_id=alert_log_id)

    return result


@require_context
def alert_log_get(context, alert_log_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        alert_log = _alert_log_get(context, alert_log_id, session)
        _alert_log_load_attr(context, alert_log, expected_attrs, session)
    return alert_log


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def alert_log_create(context, values):
    alert_log_ref = models.AlertLog()
    alert_log_ref.update(values)
    session = get_session()
    with session.begin():
        alert_log_ref.save(session)

    return alert_log_ref


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def alert_log_update(context, alert_log_id, values):
    session = get_session()
    with session.begin():
        query = _alert_log_get_query(context, session)
        result = query.filter_by(id=alert_log_id).update(values)
        if not result:
            raise exception.AlertLogNotFound(alert_log_id=alert_log_id)


def _alert_log_load_attr(ctxt, alert_log, expected_attrs=None, session=None):
    expected_attrs = expected_attrs or []
    if 'alert_rule' in expected_attrs:
        alert_rule = _alert_rule_get(ctxt, alert_log.alert_rule_id, session)
        alert_log.alert_rule = alert_rule


@require_context
def alert_log_get_all(context, marker=None, limit=None, sort_keys=None,
                      sort_dirs=None, filters=None, offset=None,
                      expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.AlertLog,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        alert_logs = query.all()
        for ale_log in alert_logs:
            _alert_log_load_attr(context, ale_log, expected_attrs, session)
        return alert_logs


@require_context
def alert_log_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _alert_log_get_query(context, session)
        query = process_filters(models.AlertLog)(query, filters)
        return query.count()


def alert_log_destroy(context, alert_log_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.AlertLog, session=session). \
            filter_by(id=alert_log_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def alert_log_batch_update(context, filters=None, updates=None):
    session = get_session()
    with session.begin():
        if filters:
            before_time = filters.get('created_at')
            model_query(context, models.AlertLog, session=session).filter(
                models.AlertLog.created_at <= before_time
            ).update(updates, synchronize_session=False)
            LOG.info('alert_log set_deleted success,filters:<=%s', before_time)
        else:

            model_query(context, models.AlertLog,
                        session=session).update(updates)
            LOG.info('alert_log set all_read success')
        return True


###############################


@require_context
def _log_file_get_query(context, session=None):
    return model_query(context, models.LogFile, session=session)


def _log_file_get(context, log_file_id, session=None):
    result = _log_file_get_query(context, session=session)
    result = result.filter_by(id=log_file_id).first()

    if not result:
        raise exception.LogFileNotFound(log_file_id=log_file_id)

    return result


@require_context
def log_file_get(context, log_file_id, expected_attrs=None):
    return _log_file_get(context, log_file_id)


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def log_file_create(context, values):
    log_file_ref = models.LogFile()
    log_file_ref.update(values)
    session = get_session()
    with session.begin():
        log_file_ref.save(session)

    return log_file_ref


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def log_file_update(context, log_file_id, values):
    session = get_session()
    with session.begin():
        query = _log_file_get_query(context, session)
        result = query.filter_by(id=log_file_id).update(values)
        if not result:
            raise exception.LogFileNotFound(log_file_id=log_file_id)


@require_context
def log_file_get_all(context, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.LogFile,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


@require_context
def log_file_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _log_file_get_query(context, session)
        query = process_filters(models.LogFile)(query, filters)
        return query.count()


def log_file_destroy(context, log_file_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.LogFile, session=session). \
            filter_by(id=log_file_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


###############################


@require_context
def _volume_snapshot_get_query(context, session=None):
    return model_query(context, models.VolumeSnapshot, session=session)


def _volume_snapshot_get(context, volume_snapshot_id, session=None):
    result = _volume_snapshot_get_query(context, session=session)
    result = result.filter_by(id=volume_snapshot_id).first()

    if not result:
        raise exception.VolumeSnapshotNotFound(
            volume_snapshot_id=volume_snapshot_id)

    return result


def _snap_load_attr(ctxt, snap, expected_attrs=None, session=None):
    expected_attrs = expected_attrs or []
    if 'volume' in expected_attrs:
        snap.volume = snap._volume
    if 'pool' in expected_attrs:
        p_volume = _volume_get(ctxt, snap.volume_id, session)
        snap.pool = _pool_get(ctxt, p_volume.pool_id, session)
    if 'child_volumes' in expected_attrs:
        child_volumes = model_query(
            ctxt, models.Volume, session=session).filter_by(
            snapshot_id=snap.id, is_link_clone=True)
        snap.child_volumes = [child_volume for child_volume in child_volumes
                              if not child_volume.deleted]


@require_context
def volume_snapshot_get(context, volume_snapshot_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        snap = _volume_snapshot_get(context, volume_snapshot_id, session)
        _snap_load_attr(context, snap, expected_attrs, session)
    return snap


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_snapshot_create(context, values):
    volume_snapshot_ref = models.VolumeSnapshot()
    volume_snapshot_ref.update(values)
    session = get_session()
    with session.begin():
        volume_snapshot_ref.save(session)

    return volume_snapshot_ref


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def volume_snapshot_update(context, volume_snapshot_id, values):
    session = get_session()
    with session.begin():
        query = _volume_snapshot_get_query(context, session)
        result = query.filter_by(id=volume_snapshot_id).update(values)
        if not result:
            raise exception.VolumeSnapshotNotFound(
                volume_snapshot_id=volume_snapshot_id)


@require_context
def volume_snapshot_get_all(context, marker=None, limit=None, sort_keys=None,
                            sort_dirs=None, filters=None, offset=None,
                            expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.VolumeSnapshot,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        volume_snapshots = query.all()
        for snapshot in volume_snapshots:
            _snap_load_attr(context, snapshot, expected_attrs, session)
        return volume_snapshots


@require_context
def volume_snapshot_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _volume_snapshot_get_query(context, session)
        query = process_filters(models.VolumeSnapshot)(query, filters)
        return query.count()


def volume_snapshot_destroy(context, volume_snapshot_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeSnapshot, session=session). \
            filter_by(id=volume_snapshot_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


###############################


def _service_get_query(context, session=None):
    if context.cluster_id:
        return model_query(
            context, models.Service, session=session
        ).filter_by(cluster_id=context.cluster_id)
    else:
        return model_query(
            context, models.Service, session=session
        )


def _service_get(context, service_id, session=None):
    result = _service_get_query(context, session)
    result = result.filter_by(id=service_id).first()

    if not result:
        raise exception.ServiceNotFound(service_id=service_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def service_create(context, values):
    service_ref = models.Service()
    service_ref.update(values)
    session = get_session()
    with session.begin():
        service_ref.save(session)
    return service_ref


@require_context
def service_destroy(context, service_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Service, session=session). \
            filter_by(id=service_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def service_get(context, service_id, expected_attrs=None):
    return _service_get(context, service_id)


@require_context
def service_get_all(context, marker=None, limit=None, sort_keys=None,
                    sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
    if filters.get("cluster_id") != "*":
        if "cluster_id" not in filters.keys():
            filters['cluster_id'] = context.cluster_id
    else:
        filters.pop("cluster_id")
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Service, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
def service_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _service_get_query(context, session)
        query = process_filters(models.Service)(query, filters)
        return query.count()


@require_context
def service_status_get(context, names, filters=None):
    """
    ?????????????????????????????????????????????????????????????????????????????????
    :param context:
    :param names: []
    :param filters: {}
    :return: {}
    """
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        all_status = {}
        for name in names:
            filters["name"] = name
            query = _service_get_query(context, session)
            query = process_filters(models.Service)(query, filters)
            active = 0
            error = 0
            inactive = 0
            if query:
                active = query.filter_by(status=ServiceStatus.ACTIVE).count()
                error = query.filter_by(status=ServiceStatus.ERROR).count()
                inactive = query.filter_by(
                    status=ServiceStatus.INACTIVE).count()
            status = {name: {
                ServiceStatus.ACTIVE: active,
                ServiceStatus.FAILED: error,
                ServiceStatus.INACTIVE: inactive
            }}
            all_status.update(status)
        return all_status


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def service_update(context, service_id, values):
    session = get_session()
    with session.begin():
        query = _service_get_query(context, session)
        result = query.filter_by(id=service_id).update(values)
        if not result:
            raise exception.ServiceNotFound(service_id=service_id)


###############################


def _ceph_config_get_query(context, session=None):
    return model_query(
        context, models.CephConfig, session=session
    ).filter_by(cluster_id=context.cluster_id)


def _ceph_config_get(context, ceph_config_id, session=None):
    result = _ceph_config_get_query(context, session)
    result = result.filter_by(id=ceph_config_id).first()

    if not result:
        raise exception.CephConfigNotFound(ceph_config_id=ceph_config_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def ceph_config_create(context, values):
    ceph_config_ref = models.CephConfig()
    ceph_config_ref.update(values)
    ceph_config_ref.cluster_id = context.cluster_id
    session = get_session()
    with session.begin():
        ceph_config_ref.save(session)
    return ceph_config_ref


@require_context
def ceph_config_destroy(context, ceph_config_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.CephConfig, session=session). \
            filter_by(id=ceph_config_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def ceph_config_get(context, ceph_config_id, expected_attrs=None):
    return _ceph_config_get(context, ceph_config_id)


@require_context
def ceph_config_get_by_key(context, group, key):
    session = get_session()
    with session.begin():
        result = _ceph_config_get_query(context, session)
        result = result.filter_by(group=group, key=key).first()

        if not result:
            return None
        return result


@require_context
def ceph_config_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _ceph_config_get_query(context, session)
        query = process_filters(models.CephConfig)(query, filters)
        return query.count()


@require_context
def ceph_config_get_all(context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.CephConfig, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def ceph_config_update(context, ceph_config_id, values):
    session = get_session()
    with session.begin():
        query = _ceph_config_get_query(context, session)
        result = query.filter_by(id=ceph_config_id).update(values)
        if not result:
            raise exception.CephConfigNotFound(ceph_config_id=ceph_config_id)


###############################

def _crush_rule_get_query(context, session=None):
    return model_query(
        context, models.CrushRule, session=session
    ).filter_by(cluster_id=context.cluster_id)


def _crush_rule_get(context, crush_rule_id, session=None):
    result = _crush_rule_get_query(context, session)
    result = result.filter_by(id=crush_rule_id).first()

    if not result:
        raise exception.CrushRuleNotFound(crush_rule_id=crush_rule_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def crush_rule_create(context, values):
    crush_rule_ref = models.CrushRule()
    crush_rule_ref.update(values)
    if "cluster_id" not in values.keys():
        crush_rule_ref.cluster_id = context.cluster_id
    session = get_session()
    with session.begin():
        crush_rule_ref.save(session)
    return crush_rule_ref


@require_context
def crush_rule_destroy(context, crush_rule_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.CrushRule, session=session). \
            filter_by(id=crush_rule_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _crush_rule_load_attr(crush_rule, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'osds' in expected_attrs:
        crush_rule.osds = [osd for osd in crush_rule._osds if not osd.deleted]


@require_context
def crush_rule_get(context, crush_rule_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        crush_rule = _crush_rule_get(context, crush_rule_id, session)
        _crush_rule_load_attr(crush_rule, expected_attrs)
    return crush_rule


@require_context
def crush_rule_get_all(context, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None,
                       expected_attrs=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.CrushRule, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        crush_rules = query.all()
        for crush_rule in crush_rules:
            _crush_rule_load_attr(crush_rule, expected_attrs)
        return crush_rules


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def crush_rule_update(context, crush_rule_id, values):
    session = get_session()
    with session.begin():
        query = _crush_rule_get_query(context, session)
        result = query.filter_by(id=crush_rule_id).update(values)
        if not result:
            raise exception.CrushRuleNotFound(crush_rule_id=crush_rule_id)


###############################


def _process_action_log_filters(query, filters):
    filters = filters.copy()
    # Holds the simple exact matches
    filter_dict = {}

    # Iterate over all filters, special case the filter if necessary
    for key, value in filters.items():
        if isinstance(value, (tuple, set, frozenset)):
            # Looking for values in a list; apply to query directly
            column_attr = getattr(models.ActionLog, key)
            query = query.filter(column_attr.in_(value))
        elif key == 'begin_time':
            query = query.filter(models.ActionLog.begin_time.between(
                value[0], value[1]))
        else:
            # OK, simple exact match; save for later
            filter_dict[key] = value

    # Apply simple exact matches
    if filter_dict:
        query = query.filter_by(**filter_dict)
    return query


@require_context
def _action_log_get_query(context, session=None):
    return model_query(context, models.ActionLog, session=session)


def _action_log_get(context, action_log_id, session=None):
    result = _action_log_get_query(context, session=session)
    result = result.filter_by(id=action_log_id).first()

    if not result:
        raise exception.ActionLogNotFound(action_log_id=action_log_id)

    return result


@require_context
def action_log_get(context, action_log_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        action_log = _action_log_get(context, action_log_id, session)
        _action_log_load_attr(context, action_log, expected_attrs, session)
    return action_log


def action_log_destroy(context, action_log_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.ActionLog, session=session). \
            filter_by(id=action_log_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def action_log_create(context, values):
    action_log_ref = models.ActionLog()
    action_log_ref.update(values)
    session = get_session()
    with session.begin():
        action_log_ref.save(session)

    return action_log_ref


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def action_log_update(context, action_log_id, values):
    session = get_session()
    with session.begin():
        query = _action_log_get_query(context, session)
        result = query.filter_by(id=action_log_id).update(values)
        if not result:
            raise exception.ActionLogNotFound(action_log_id=action_log_id)


def _action_log_load_attr(ctxt, action_log, expected_attrs=None,
                          session=None):
    expected_attrs = expected_attrs or []
    if 'user' in expected_attrs:
        user = _user_get(ctxt, action_log.user_id, session)
        action_log.user = user


@require_context
def action_log_get_all(context, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None,
                       expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.ActionLog,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        action_logs = query.all()
        for action_log in action_logs:
            _action_log_load_attr(context, action_log, expected_attrs, session)
        return action_logs


@require_context
def action_log_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _action_log_get_query(context, session)
        query = process_filters(models.ActionLog)(query, filters)
        return query.count()


###############################


def _user_get_query(context, session=None):
    return model_query(context, models.User, session=session)


def _user_get(context, user_id, session=None):
    result = _user_get_query(context, session=session)
    result = result.filter_by(id=user_id).first()

    if not result:
        raise exception.UserNotFound(user_id=user_id)

    return result


@require_context
def user_get(context, user_id, expected_attrs=None):
    return _user_get(context, user_id)


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def user_create(context, values):
    user_ref = models.User()
    user_ref.update(values)
    session = get_session()
    with session.begin():
        user_ref.save(session)

    return user_ref


@handle_db_data_error
@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def user_update(context, user_id, values):
    session = get_session()
    with session.begin():
        query = _user_get_query(context, session)
        result = query.filter_by(id=user_id).update(values)
        if not result:
            raise exception.UserNotFound(user_id=user_id)


def user_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None,
                 expected_attrs=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.User,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


@require_context
def user_get_count(context, filters=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _user_get_query(context, session)
        query = process_filters(models.User)(query, filters)
        return query.count()


###############################
def _taskflow_get_query(context, session=None):
    return model_query(context, models.Taskflow, session=session)


def _taskflow_get(context, taskflow_id, session=None):
    result = _taskflow_get_query(context, session)
    result = result.filter_by(id=taskflow_id).first()

    if not result:
        raise exception.TaskflowNotFound(taskflow_id=taskflow_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def taskflow_create(context, values):
    taskflow_ref = models.Taskflow()
    taskflow_ref.update(values)
    if "cluster_id" not in values:
        taskflow_ref.cluster_id = context.cluster_id
    session = get_session()
    with session.begin():
        taskflow_ref.save(session)

    return taskflow_ref


def taskflow_destroy(context, taskflow_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Taskflow, session=session). \
            filter_by(id=taskflow_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def taskflow_get(context, taskflow_id, expected_attrs=None):
    return _taskflow_get(context, taskflow_id)


@require_context
def taskflow_get_all(context, marker=None, limit=None, sort_keys=None,
                     sort_dirs=None, filters=None, offset=None,
                     expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Taskflow, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def taskflow_update(context, taskflow_id, values):
    session = get_session()
    with session.begin():
        query = _taskflow_get_query(context, session)
        result = query.filter_by(id=taskflow_id).update(values)
        if not result:
            raise exception.TaskflowNotFound(taskflow_id=taskflow_id)


@require_context
def taskflow_get_count(context, filters=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _taskflow_get_query(context, session)
        query = process_filters(models.Taskflow)(query, filters)
        return query.count()


###############################


def _task_get_query(context, session=None):
    return model_query(context, models.Task, session=session)


def _task_get(context, task_id, session=None):
    result = _task_get_query(context, session)
    result = result.filter_by(id=task_id).first()

    if not result:
        raise exception.TaskNotFound(task_id=task_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def task_create(context, values):
    task_ref = models.Task()
    task_ref.update(values)
    if "cluster_id" not in values:
        task_ref.cluster_id = context.cluster_id
    session = get_session()
    with session.begin():
        task_ref.save(session)

    return task_ref


def task_destroy(context, task_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Task, session=session). \
            filter_by(id=task_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def task_get(context, task_id, expected_attrs=None):
    return _task_get(context, task_id)


@require_context
def task_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None,
                 expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Task, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def task_update(context, task_id, values):
    session = get_session()
    with session.begin():
        query = _task_get_query(context, session)
        result = query.filter_by(id=task_id).update(values)
        if not result:
            raise exception.TaskNotFound(task_id=task_id)


@require_context
def task_get_count(context, filters=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _task_get_query(context, session)
        query = process_filters(models.Task)(query, filters)
        return query.count()


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
    @apply_like_filters(model)
    def _process_filters(query, filters):
        if filters:
            # Ensure that filters' keys exist on the model
            if not is_valid_model_filters(model, filters):
                return

            where_conds = []
            for field, condition in filters.items():
                if not isinstance(condition, db.Condition):
                    condition = db.Condition(condition, field)
                where_conds.append(condition.get_filter(model, field))

            if where_conds:
                query = query.filter(*where_conds)

        return query

    return _process_filters


###############################


def _radosgw_get_query(context, session=None):
    return model_query(context, models.Radosgw, session=session)


def _radosgw_get(context, radosgw_id, session=None):
    result = _radosgw_get_query(context, session)
    result = result.filter_by(id=radosgw_id).first()

    if not result:
        raise exception.RadosgwNotFound(radosgw_id=radosgw_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def radosgw_create(context, values):
    radosgw_ref = models.Radosgw()
    radosgw_ref.update(values)
    session = get_session()
    with session.begin():
        radosgw_ref.save(session)

    return radosgw_ref


def radosgw_destroy(context, radosgw_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.Radosgw, session=session). \
            filter_by(id=radosgw_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _radosgw_load_attr(ctxt, radosgw, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'node' in expected_attrs:
        radosgw.node = radosgw._node


@require_context
def radosgw_get(context, radosgw_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        radosgw = _radosgw_get(context, radosgw_id, session)
        _radosgw_load_attr(context, radosgw, expected_attrs)
        return radosgw


@require_context
def radosgw_get_all(context, marker=None, limit=None, sort_keys=None,
                    sort_dirs=None, filters=None, offset=None,
                    expected_attrs=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.Radosgw, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        radosgws = query.all()
        if not expected_attrs:
            return radosgws
        for radosgw in radosgws:
            _radosgw_load_attr(context, radosgw, expected_attrs)
        return radosgws


@require_context
def radosgw_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _radosgw_get_query(context, session)
        query = process_filters(models.Radosgw)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def radosgw_update(context, radosgw_id, values):
    session = get_session()
    with session.begin():
        query = _radosgw_get_query(context, session)
        result = query.filter_by(id=radosgw_id).update(values)
        if not result:
            raise exception.RadosgwNotFound(radosgw_id=radosgw_id)


###############################


def _radosgw_zone_get_query(context, session=None):
    return model_query(context, models.RadosgwZone, session=session)


def _radosgw_zone_get(context, rgw_zone_id, session=None):
    result = _radosgw_zone_get_query(context, session)
    result = result.filter_by(id=rgw_zone_id).first()

    if not result:
        raise exception.RgwZoneNotFound(rgw_zone_id=rgw_zone_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def radosgw_zone_create(context, values):
    radosgw_zone_ref = models.RadosgwZone()
    radosgw_zone_ref.update(values)
    session = get_session()
    with session.begin():
        radosgw_zone_ref.save(session)

    return radosgw_zone_ref


def radosgw_zone_destroy(context, rgw_zone_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.RadosgwZone, session=session). \
            filter_by(id=rgw_zone_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _radosgw_zone_load_attr(ctxt, radosgw_zone, expected_attrs=None):
    pass


@require_context
def radosgw_zone_get(context, rgw_zone_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        radosgw_zone = _radosgw_zone_get(context, rgw_zone_id, session)
        _radosgw_zone_load_attr(context, rgw_zone_id, expected_attrs)
        return radosgw_zone


@require_context
def radosgw_zone_get_all(context, marker=None, limit=None, sort_keys=None,
                         sort_dirs=None, filters=None, offset=None,
                         expected_attrs=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.RadosgwZone, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        radosgw_zones = query.all()
        if not expected_attrs:
            return radosgw_zones
        for radosgw_zone in radosgw_zones:
            _radosgw_zone_load_attr(context, radosgw_zone, expected_attrs)
        return radosgw_zones


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def radosgw_zone_update(context, rgw_zone_id, values):
    session = get_session()
    with session.begin():
        query = _radosgw_zone_get_query(context, session)
        result = query.filter_by(id=rgw_zone_id).update(values)
        if not result:
            raise exception.RgwZoneNotFound(rgw_zone_id=rgw_zone_id)


########################


def _radosgw_router_get_query(context, session=None):
    return model_query(context, models.RadosgwRouter, session=session)


def _radosgw_router_get(context, rgw_router_id, session=None):
    result = _radosgw_router_get_query(context, session)
    result = result.filter_by(id=rgw_router_id).first()

    if not result:
        raise exception.RgwRouterNotFound(rgw_router_id=rgw_router_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def radosgw_router_create(context, values):
    radosgw_ref = models.RadosgwRouter()
    radosgw_ref.update(values)
    session = get_session()
    with session.begin():
        radosgw_ref.save(session)

    return radosgw_ref


def radosgw_router_destroy(context, rgw_router_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.RadosgwRouter, session=session). \
            filter_by(id=rgw_router_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _radosgw_router_load_attr(ctxt, rgw_router, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'radosgws' in expected_attrs:
        rgw_router.radosgws = [rgw for rgw in rgw_router._radosgws
                               if not rgw.deleted]
    if 'router_services' in expected_attrs:
        rgw_router.router_services = [service for service in
                                      rgw_router._router_services
                                      if not service.deleted]


@require_context
def radosgw_router_get(context, rgw_router_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        rgw_router = _radosgw_router_get(context, rgw_router_id, session)
        _radosgw_router_load_attr(context, rgw_router, expected_attrs)
        return rgw_router


@require_context
def radosgw_router_get_all(context, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None,
                           expected_attrs=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.RadosgwRouter, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        rgw_routers = query.all()
        if not expected_attrs:
            return rgw_routers
        for rgw_router in rgw_routers:
            _radosgw_router_load_attr(context, rgw_router, expected_attrs)
        return rgw_routers


@require_context
def radosgw_router_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _radosgw_router_get_query(context, session)
        query = process_filters(models.RadosgwRouter)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def radosgw_router_update(context, rgw_router_id, values):
    session = get_session()
    with session.begin():
        query = _radosgw_router_get_query(context, session)
        result = query.filter_by(id=rgw_router_id).update(values)
        if not result:
            raise exception.RgwRouterNotFound(rgw_router_id=rgw_router_id)


########################


def _router_service_get_query(context, session=None):
    return model_query(
        context, models.RouterService, session=session)


def _router_service_get(context, router_service_id, session=None):
    result = _router_service_get_query(context, session)
    result = result.filter_by(id=router_service_id).first()

    if not result:
        raise exception.ServiceNotFound(service_id=router_service_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def router_service_create(context, values):
    router_service_ref = models.RouterService()
    router_service_ref.update(values)
    session = get_session()
    with session.begin():
        router_service_ref.save(session)
    return router_service_ref


@require_context
def router_service_destroy(context, router_service_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.RouterService, session=session). \
            filter_by(id=router_service_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def router_service_get(context, router_service_id, expected_attrs=None):
    return _router_service_get(context, router_service_id)


@require_context
def router_service_get_all(context, marker=None, limit=None, sort_keys=None,
                           sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.RouterService, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        return query.all()


@require_context
def router_service_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _router_service_get_query(context, session)
        query = process_filters(models.RouterService)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def router_service_update(context, router_service_id, values):
    session = get_session()
    with session.begin():
        query = _router_service_get_query(context, session)
        result = query.filter_by(id=router_service_id).update(values)
        if not result:
            raise exception.ServiceNotFound(service_id=router_service_id)

########################


def _object_policy_get_query(context, session=None):
    return model_query(
        context, models.ObjectPolicy, session=session)


def _object_policy_get(context, object_policy_id, session=None):
    result = _object_policy_get_query(context, session)
    result = result.filter_by(id=object_policy_id).first()
    if not result:
        raise exception.ObjectPolicyNotFound(object_policy_id=object_policy_id)
    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_policy_create(context, values):
    object_policy_ref = models.ObjectPolicy()
    object_policy_ref.update(values)
    session = get_session()
    with session.begin():
        object_policy_ref.save(session)
    return object_policy_ref


@require_context
def object_policy_destroy(context, object_policy_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.ObjectPolicy, session=session). \
            filter_by(id=object_policy_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _object_policy_load_attr(ctxt, object_policy, expected_attrs=None,
                             session=None):
    expected_attrs = expected_attrs or []
    if 'index_pool' in expected_attrs:
        object_policy.index_pool = object_policy._index_pool
    if 'data_pool' in expected_attrs:
        object_policy.data_pool = object_policy._data_pool
    if 'buckets' in expected_attrs:
        object_policy.buckets = [bucket for bucket in object_policy._buckets
                                 if not bucket.deleted]


@require_context
def object_policy_get(context, object_policy_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        object_policy = _object_policy_get(context, object_policy_id, session)
        _object_policy_load_attr(context, object_policy, expected_attrs,
                                 session)
    return object_policy


@require_context
def object_policy_get_all(context, marker=None, limit=None, sort_keys=None,
                          sort_dirs=None, filters=None, offset=None,
                          expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.ObjectPolicy, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        object_policies = query.all()
        for object_policy in object_policies:
            _object_policy_load_attr(context, object_policy, expected_attrs,
                                     session)
        return object_policies


@require_context
def object_policy_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _object_policy_get_query(context, session)
        query = process_filters(models.ObjectPolicy)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_policy_update(context, object_policy_id, values):
    session = get_session()
    with session.begin():
        query = _object_policy_get_query(context, session)
        result = query.filter_by(id=object_policy_id).update(values)
        if not result:
            raise exception.ObjectPolicyNotFound(
                object_policy_id=object_policy_id)


########################

def _object_user_get_query(context, session=None):
    return model_query(
        context, models.ObjectUser, session=session)


def _object_user_get(context, object_user_id, session=None):
    result = _object_user_get_query(context, session)
    result = result.filter_by(id=object_user_id).first()
    if not result:
        raise exception.ObjectUserNotFound(object_user_id=object_user_id)
    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_user_create(context, values):
    object_user_ref = models.ObjectUser()
    object_user_ref.update(values)
    session = get_session()
    with session.begin():
        object_user_ref.save(session)
    return object_user_ref


@require_context
def object_user_destroy(context, object_user_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.ObjectUser, session=session). \
            filter_by(id=object_user_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _object_user_load_attr(ctxt, object_user, expected_attrs=None,
                           session=None):
    expected_attrs = expected_attrs or []
    if 'access_keys' in expected_attrs:
        object_user.access_keys = [access_key for access_key in
                                   object_user._access_keys if not
                                   access_key.deleted]


@require_context
def object_user_get(context, object_user_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        object_user = _object_user_get(context, object_user_id, session)
        _object_user_load_attr(context, object_user, expected_attrs,
                               session)
    return object_user


@require_context
def object_user_get_all(context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None,
                        expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.ObjectUser, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        object_users = query.all()
        for object_user in object_users:
            _object_user_load_attr(context, object_user, expected_attrs,
                                   session)
        return object_users


@require_context
def object_user_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _object_user_get_query(context, session)
        query = process_filters(models.ObjectUser)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_user_update(context, object_user_id, values):
    session = get_session()
    with session.begin():
        query = _object_user_get_query(context, session)
        result = query.filter_by(id=object_user_id).update(values)
        if not result:
            raise exception.ObjectUserNotFound(
                object_user_id=object_user_id)


########################

def _object_access_key_get_query(context, session=None):
    return model_query(
        context, models.ObjectAccessKey, session=session)


def _object_access_key_get(context, object_access_key_id, session=None):
    result = _object_access_key_get_query(context, session)
    result = result.filter_by(id=object_access_key_id).first()
    if not result:
        raise exception.ObjectAccessKeyNotFound(
            object_access_key_id=object_access_key_id)
    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_access_key_create(context, values):
    object_access_key_ref = models.ObjectAccessKey()
    object_access_key_ref.update(values)
    session = get_session()
    with session.begin():
        object_access_key_ref.save(session)
    return object_access_key_ref


@require_context
def object_access_key_destroy(context, object_access_key_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.ObjectAccessKey, session=session). \
            filter_by(id=object_access_key_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _object_access_key_load_attr(ctxt, object_access_key, expected_attrs=None,
                                 session=None):
    expected_attrs = expected_attrs or []
    if 'obj_user' in expected_attrs:
        object_access_key.obj_user = object_access_key._object_user


@require_context
def object_access_key_get(context, object_access_key_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        object_access_key = _object_access_key_get(
            context, object_access_key_id)
    return object_access_key


@require_context
def object_access_key_get_all(context, marker=None, limit=None, sort_keys=None,
                              sort_dirs=None, filters=None, offset=None,
                              expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.ObjectAccessKey, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        object_access_keys = query.all()
        for object_access_key in object_access_keys:
            _object_access_key_load_attr(context, object_access_key,
                                         expected_attrs, session)
        return object_access_keys


@require_context
def object_access_key_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _object_access_key_get_query(context, session)
        query = process_filters(models.ObjectAccessKey)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_access_key_update(context, object_access_key_id, values):
    session = get_session()
    with session.begin():
        query = _object_access_key_get_query(context, session)
        result = query.filter_by(id=object_access_key_id).update(values)
        if not result:
            raise exception.ObjectAccessKeyNotFound(
                object_access_key_id=object_access_key_id)


########################


def _object_bucket_get_query(context, session=None):
    return model_query(
        context, models.ObjectBucket, session=session)


def _object_bucket_get(context, object_user_id, session=None):
    result = _object_bucket_get_query(context, session)
    result = result.filter_by(id=object_user_id).first()

    if not result:
        raise exception.ObjectBucketNotFound(
            object_bucket_id=object_user_id)

    return result


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_bucket_create(context, values):
    object_policy_ref = models.ObjectBucket()
    object_policy_ref.update(values)
    session = get_session()
    with session.begin():
        object_policy_ref.save(session)
    return object_policy_ref


@require_context
def object_bucket_destroy(context, object_user_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.ObjectBucket, session=session). \
            filter_by(id=object_user_id). \
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _object_bucket_load_attr(ctxt, object_bucket, expected_attrs=None,
                             session=None):
    expected_attrs = expected_attrs or []
    if 'owner' in expected_attrs:
        object_bucket.owner = object_bucket._object_user
    if 'policy' in expected_attrs:
        object_bucket.policy = object_bucket._object_policy
    if 'lifecycles' in expected_attrs:
        object_bucket.lifecycles = [lifecycle for lifecycle in
                                    object_bucket._lifecycles
                                    if not lifecycle.deleted]


@require_context
def object_bucket_get(context, object_policy_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        object_policy = _object_bucket_get(context, object_policy_id,
                                           session)
        _object_bucket_load_attr(context, object_policy, expected_attrs,
                                 session)
    return object_policy


@require_context
def object_bucket_get_all(context, marker=None, limit=None, sort_keys=None,
                          sort_dirs=None, filters=None, offset=None,
                          expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.ObjectBucket, marker, limit,
            sort_keys, sort_dirs, filters,
            offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        object_buckets = query.all()
        for object_bucket in object_buckets:
            _object_bucket_load_attr(context, object_bucket,
                                     expected_attrs, session)
        return object_buckets


@require_context
def object_bucket_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _object_bucket_get_query(context, session)
        query = process_filters(models.ObjectBucket)(query, filters)
        return query.count()


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_bucket_update(context, object_policy_id, values):
    session = get_session()
    with session.begin():
        query = _object_bucket_get_query(context, session)
        result = query.filter_by(id=object_policy_id).update(values)
        if not result:
            raise exception.ObjectBucketNotFound(
                object_bucket_id=object_policy_id)


########################


def _object_lifecycle_get_query(context, session=None):
    return model_query(
        context, models.ObjectLifecycle, session=session)


def _object_lifecycle_get(context, object_lifecycle_id, session=None):
    result = _object_lifecycle_get_query(context, session)
    result = result.filter_by(id=object_lifecycle_id).first()

    if not result:
        raise exception.ObjectLifecycleNotFound(
            object_lifecycle_id=object_lifecycle_id)
    return result


@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_lifecycle_create(context, values):
    object_lifecycle_ref = models.ObjectLifecycle()
    object_lifecycle_ref.update(values)
    session = get_session()
    with session.begin():
        object_lifecycle_ref.save(session)
    return object_lifecycle_ref


@require_context
def object_lifecycle_destroy(context, object_lifecycle_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.ObjectLifecycle, session=session). \
            filter_by(id=object_lifecycle_id). \
            update(updated_values)
        del updated_values['updated_at']
        return updated_values


def _object_lifecycle_load_attr(ctxt, object_lifecycle, expected_attrs=None,
                                session=None):
    expected_attrs = expected_attrs or []
    if 'bucket' in expected_attrs:
        object_lifecycle.bucket = object_lifecycle._object_bucket


@require_context
def object_lifecycle_get(context, object_lifecycle_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        object_lifecycle = _object_lifecycle_get(
            context, object_lifecycle_id, session)
        _object_lifecycle_load_attr(context, object_lifecycle, expected_attrs,
                                    session)
    return object_lifecycle


@require_context
def object_lifecycle_get_all(context, marker=None, limit=None, sort_keys=None,
                             sort_dirs=None, filters=None, offset=None,
                             expected_attrs=None):
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.ObjectLifecycle, marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No clusters would match, return empty list
        if query is None:
            return []
        object_lifecycles = query.all()
        for object_lifecycle in object_lifecycles:
            _object_lifecycle_load_attr(context, object_lifecycle,
                                        expected_attrs, session)
        return object_lifecycles


@require_context
def object_lifecycle_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    if "cluster_id" not in filters.keys():
        filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _object_lifecycle_get_query(context, session)
        query = process_filters(models.ObjectLifecycle)(query, filters)
        return query.count()


@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def object_lifecycle_update(context, object_lifecycle_id, values):
    session = get_session()
    with session.begin():
        query = _object_lifecycle_get_query(context, session)
        result = query.filter_by(id=object_lifecycle_id).update(values)
        if not result:
            raise exception.ObjectLifecycleNotFound(
                object_lifecycle_id=object_lifecycle_id)


########################


@require_context
@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def logo_create(context, values):
    object_logo = models.Logo()
    object_logo.update(values)
    session = get_session()
    with session.begin():
        object_logo.save(session)
    return object_logo


def _logo_get_query(context, session=None):
    return model_query(
        context, models.Logo, session=session)


def _logo_get(context, logo_name, session=None):
    result = _logo_get_query(context, session)
    result = result.filter_by(name=logo_name).first()

    if not result:
        raise exception.LogoNotFound(logo_name=logo_name)
    return result


@require_context
def logo_get(context, logo_name, expected_attrs=None):
    session = get_session()
    with session.begin():
        logo = _logo_get(
            context, logo_name, session)
    return logo


@oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
def logo_update(context, logo_name, values):
    session = get_session()
    with session.begin():
        query = _logo_get_query(context, session)
        result = query.filter_by(name=logo_name).update(values)
        if not result:
            raise exception.LogoNotFound(logo_name=logo_name)


@require_context
def logo_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None,
                 expected_attrs=None):
    filters = filters or {}
    if "cluster_id" in filters.keys():
        filters.pop('cluster_id')
    session = get_session()
    with session.begin():
        query = _logo_get_query(context, session)
        if query is None:
            return []
        return query


########################


PAGINATION_HELPERS = {
    models.Node: (_node_get_query, process_filters(models.Node), _node_get),
    models.Volume: (_volume_get_query, process_filters(models.Volume),
                    _volume_get),
    models.Osd: (_osd_get_query, process_filters(models.Osd), _osd_get),
    models.Cluster: (_cluster_get_query, _process_cluster_filters,
                     _cluster_get),
    models.RPCService: (_rpc_service_get_query,
                        process_filters(models.RPCService), _rpc_service_get),
    models.VolumeAccessPath: (_volume_access_path_get_query,
                              process_filters(models.VolumeAccessPath),
                              _volume_access_path_get),
    models.VolumeGateway: (_volume_gateway_get_query,
                           process_filters(models.VolumeGateway),
                           _volume_gateway_get),
    models.VolumeClient: (_volume_client_get_query,
                          process_filters(models.VolumeClient),
                          _volume_client_get),
    models.VolumeClientGroup: (_volume_client_group_get_query,
                               process_filters(models.VolumeClientGroup),
                               _volume_client_group_get),
    models.VolumeMapping: (_volume_mapping_get_query,
                           process_filters(models.VolumeMapping),
                           _volume_mapping_get),
    models.LicenseFile: (_license_get_query, _process_license_filters,
                         _license_get),
    models.AlertRule: (_alert_rule_get_query,
                       process_filters(models.AlertRule),
                       _alert_rule_get),
    models.Network: (_network_get_query,
                     process_filters(models.Network),
                     _network_get),
    models.SysConfig: (_sys_config_get_query,
                       process_filters(models.SysConfig),
                       _sys_config_get),
    models.Datacenter: (_datacenter_get_query,
                        process_filters(models.Datacenter),
                        _datacenter_get),
    models.Rack: (_rack_get_query, process_filters(models.Rack), _rack_get),
    models.Disk: (_disk_get_query, process_filters(models.Disk),
                  _disk_get),
    models.DiskPartition: (_disk_partition_get_query,
                           process_filters(models.DiskPartition),
                           _disk_partition_get),
    models.EmailGroup: (_email_group_get_query,
                        process_filters(models.EmailGroup),
                        _email_group_get),
    models.AlertGroup: (_alert_group_get_query,
                        process_filters(models.AlertGroup),
                        _alert_group_get),
    models.Pool: (_pool_get_query,
                  pool_filters,
                  _pool_get),
    models.AlertLog: (_alert_log_get_query,
                      process_filters(models.AlertLog),
                      _alert_log_get),
    models.LogFile: (_log_file_get_query,
                     process_filters(models.LogFile),
                     _log_file_get),
    models.Service: (_service_get_query,
                     process_filters(models.Service),
                     _service_get),
    models.VolumeSnapshot: (_volume_snapshot_get_query,
                            process_filters(models.VolumeSnapshot),
                            _volume_snapshot_get),
    models.CrushRule: (_crush_rule_get_query,
                       process_filters(models.CrushRule),
                       _crush_rule_get),
    models.CephConfig: (_ceph_config_get_query,
                        process_filters(models.CephConfig),
                        _ceph_config_get),
    models.ActionLog: (_action_log_get_query,
                       _process_action_log_filters,
                       _action_log_get),
    models.User: (_user_get_query,
                  process_filters(models.User),
                  _user_get),
    models.Taskflow: (_taskflow_get_query,
                      process_filters(models.Taskflow),
                      _taskflow_get),
    models.Task: (_task_get_query,
                  process_filters(models.Task),
                  _task_get),
    models.Radosgw: (_radosgw_get_query,
                     process_filters(models.Radosgw),
                     _radosgw_get),
    models.RadosgwZone: (_radosgw_zone_get_query,
                         process_filters(models.RadosgwZone),
                         _radosgw_zone_get),
    models.RadosgwRouter: (_radosgw_router_get_query,
                           process_filters(models.RadosgwRouter),
                           _radosgw_router_get),
    models.RouterService: (_router_service_get_query,
                           process_filters(models.RouterService),
                           _router_service_get),
    models.ObjectPolicy: (_object_policy_get_query,
                          process_filters(models.ObjectPolicy),
                          _object_policy_get),
    models.ObjectUser: (_object_user_get_query,
                        process_filters(models.ObjectUser),
                        _object_user_get),
    models.ObjectAccessKey: (_object_access_key_get_query,
                             process_filters(models.ObjectAccessKey),
                             _object_access_key_get),
    models.ObjectBucket: (_object_bucket_get_query,
                          process_filters(models.ObjectBucket),
                          _object_bucket_get),
    models.ObjectLifecycle: (_object_lifecycle_get_query,
                             process_filters(models.ObjectLifecycle),
                             _object_lifecycle_get),
}


@require_context
def resource_exists(context, model, resource_id, session=None):
    conditions = [model.id == resource_id]
    # Match non deleted resources by the id
    if 'no' == context.read_deleted:
        conditions.append(~model.deleted)
    session = session or get_session()
    query = session.query(sql.exists().where(and_(*conditions)))
    return query.scalar()


def get_model_for_versioned_object(versioned_object):
    if isinstance(versioned_object, six.string_types):
        model_name = versioned_object
    else:
        model_name = versioned_object.obj_name()
    if model_name == "License":
        return getattr(models, "LicenseFile")
    return getattr(models, model_name)


def _get_get_method(model):
    # Exceptions to model to get methods, in general method names are a simple
    # conversion changing ORM name from camel case to snake format and adding
    # _get to the string
    get_exceptions = {}

    if model in get_exceptions:
        return get_exceptions[model]

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

    if (auto_none and
            ((isinstance(value, Iterable) and
              not isinstance(value, six.string_types) and
              None not in value) or
             (value is not None))):
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
                       include_deleted='no', order=None):
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
    query = model_query(context, model, read_deleted=include_deleted
                        ).filter(*where_conds)

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
