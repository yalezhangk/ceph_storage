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
        model_query(context, models.Volume, session=session).\
            filter_by(id=access_path_id).\
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
        def _decorator(query, filters):
            exact_filters = filters.copy()
            regex_filters = {}
            for key, value in filters.items():
                # NOTE(tommylikehu): For inexact match, the filter keys
                # are in the format of 'key~=value'
                if key.endswith('~'):
                    exact_filters.pop(key)
                    regex_filters[key.rstrip('~')] = value
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


def _volume_load_attr(volume, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'snapshots' in expected_attrs:
        volume.snapshots = [snapshot for snapshot in volume._snapshots]


@require_context
def volume_get(context, volume_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        volume = _volume_get(context, volume_id, session)
        _volume_load_attr(volume, expected_attrs)
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
            _volume_load_attr(volume, expected_attrs)
        return volumes


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
        query = _volume_get_query(context, session)
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
        rpc_service_ref.save(session)

    return rpc_service_ref


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
    ).filter_by(cluster_id=context.cluster_id)


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
        model_query(context, models.Node, session=session).\
            filter_by(id=node_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _node_load_attr(node, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'disks' in expected_attrs:
        node.disks = [disk for disk in node._disks]
    if 'networks' in expected_attrs:
        node.networks = [net for net in node._networks]
    if 'osds' in expected_attrs:
        node.osds = [osd for osd in node._osds]


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
    filters['cluster_id'] = context.cluster_id
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
    session = get_session()
    datacenter_ref = models.Datacenter()
    datacenter_ref.update(values)
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
        model_query(context, models.Datacenter, session=session).\
            filter_by(id=datacenter_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def datacenter_get(context, datacenter_id, expected_attrs=None):
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
    session = get_session()
    rack_ref = models.Rack()
    rack_ref.update(values)
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
        model_query(context, models.Rack, session=session).\
            filter_by(id=rack_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def rack_get(context, rack_id, expected_attrs=None):
    return _rack_get(context, rack_id)


@require_context
def rack_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
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


def _osd_load_attr(osd, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'node' in expected_attrs:
        osd.node = osd._node
    if 'disk' in expected_attrs:
        osd.disk = osd._disk
    if 'pools' in expected_attrs:
        crush = osd._crush_rule
        osd.pools = crush._pools
    if 'cache_partition' in expected_attrs:
        osd.cache_partition = osd._cache_partition
    if 'db_partition' in expected_attrs:
        osd.db_partition = osd._db_partition
    if 'wal_partition' in expected_attrs:
        osd.wal_partition = osd._wal_partition
    if 'journal_partition' in expected_attrs:
        osd.journal_partition = osd._journal_partition


@require_context
def osd_create(context, values):
    osd_ref = models.Osd()
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
        model_query(context, models.Osd, session=session).\
            filter_by(id=osd_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def osd_get(context, osd_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        osd = _osd_get(context, osd_id, session)
        _osd_load_attr(osd, expected_attrs)
        return osd


@require_context
def osd_get_all(context, marker=None, limit=None, sort_keys=None,
                sort_dirs=None, filters=None, offset=None,
                expected_attrs=None):
    session = get_session()
    filters = filters or {}
    filters['cluster_id'] = context.cluster_id
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
    filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _osd_get_query(context, session)
        process_filters(models.Osd)(query, filters)
        return query.count()


@require_context
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
def pool_create(context, values):
    pool_ref = models.Pool()
    pool_ref.update(values)
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
        model_query(context, models.Pool, session=session).\
            filter_by(id=pool_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _pool_load_attr(ctxt, pool, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'crush_rule' in expected_attrs:
        pool.crush_rule = pool._crush_rule
    if 'osds' in expected_attrs:
        pool.osds = [osd for osd in pool.crush_rule._osds]
    if 'volumes' in expected_attrs:
        filters = {"pool_id": pool.id}
        volumes = volume_get_all(ctxt, filters=filters)
        pool.volumes = [volume for volume in volumes]


@require_context
def pool_get(context, pool_id, expected_attrs=None):
    session = get_session()
    with session.begin():
        pool = _pool_get(context, pool_id, session)
        _pool_load_attr(context, pool, expected_attrs)
        return pool


@require_context
def pool_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None,
                 expected_attrs=None):
    session = get_session()
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
    return model_query(
        context,
        models.SysConfig,
        session=session).filter_by(cluster_id=context.cluster_id)


def _sys_config_get(context, sys_config_id, session=None):
    result = _sys_config_get_query(context, session)
    result = result.filter_by(id=sys_config_id).first()

    if not result:
        raise exception.SysConfigNotFound(sys_config_id=sys_config_id)

    return result


def sys_config_get_by_key(context, key, session=None):
    result = _sys_config_get_query(context, session).filter_by(key=key).first()

    if not result:
        return None

    return result


@require_context
def sys_config_create(context, values):
    # TODO
    # get cluster id from context
    values['cluster_id'] = context.cluster_id
    sys_config_ref = models.SysConfig()
    sys_config_ref.update(values)
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
        model_query(context, models.SysConfig, session=session).\
            filter_by(id=sys_config_id).\
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
        updated_values = {'status': 'deleted',
                          'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeAccessPath, session=session).\
            filter_by(id=access_path_id).\
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


@require_context
def volume_access_path_get(context, access_path_id, expected_attrs=None):
    return _volume_access_path_get(context, access_path_id)


@require_context
def volume_access_path_get_all(context, marker=None,
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
            context, session, models.VolumeAccessPath,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No volumes would match, return empty list
        if query is None:
            return []
        return query.all()


@handle_db_data_error
@require_context
def volume_access_path_update(context, access_path_id, values):
    session = get_session()
    with session.begin():
        query = _volume_access_path_get_query(
            context, session, joined_load=False)
        result = query.filter_by(id=access_path_id).update(values)
        if not result:
            raise exception.VolumeAccessPathNotFound(
                access_path_id=access_path_id)


@handle_db_data_error
@require_context
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
        updated_values = {'status': 'deleted',
                          'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeGateway, session=session).\
            filter_by(id=ap_gateway_id).\
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
        raise exception.VolumeGatewayNotFound(ap_gateway_id=ap_gateway_id)

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
def volume_gateways_update(context, values_list):
    session = get_session()
    with session.begin():
        volume_gateways_ref = []
        for values in values_list:
            ap_gateway_id = values['id']
            values.pop('id')
            volume_gateway_ref = _volume_access_path_get(
                context,
                ap_gateway_id,
                session=session)
            volume_gateway_ref.update(values)
            volume_gateways_ref.append(volume_gateway_ref)

        return volume_gateways_ref


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
        model_query(context, models.VolumeClient, session=session).\
            filter_by(id=volume_client_id).\
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
        model_query(context, models.VolumeClientGroup, session=session).\
            filter_by(id=client_group_id).\
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


@require_context
def volume_client_group_get(context, client_group_id, expected_attrs=None):
    return _volume_client_group_get(context, client_group_id)


@require_context
def volume_client_group_get_all(context, marker=None,
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
            context, session, models.VolumeClientGroup,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        # No volumes would match, return empty list
        if query is None:
            return []
        return query.all()


@handle_db_data_error
@require_context
def volume_client_group_update(context, client_group_id, values):
    session = get_session()
    with session.begin():
        query = _volume_client_group_get_query(
            context, session, joined_load=False)
        result = query.filter_by(id=client_group_id).update(values)
        if not result:
            raise exception.VolumeClientGroupNotFound(
                client_group_id=client_group_id)


@handle_db_data_error
@require_context
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
def license_create(context, values):
    license_ref = models.LicenseFile()
    license_ref.update(values)
    session = get_session()
    with session.begin():
        license_ref.save(session)

    return license_ref


@handle_db_data_error
@require_context
def license_update(context, license_id, values):
    session = get_session()
    with session.begin():
        query = _license_get_query(context, session)
        result = query.filter_by(id=license_id).update(values)
        if not result:
            raise exception.LicenseNotFound(license_id=license_id)


@require_context
def license_get_latest_valid(context, marker=None, limit=None, sort_keys=None,
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
    return model_query(
        context, models.Network, session=session
    ).filter_by(cluster_id=context.cluster_id)


def _network_get(context, net_id, session=None):
    result = _network_get_query(context, session)
    result = result.filter_by(id=net_id).first()

    if not result:
        raise exception.NetworkNotFound(net_id=net_id)

    return result


@require_context
def network_create(context, values):
    net_ref = models.Network()
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
        model_query(context, models.Network, session=session).\
            filter_by(id=net_id).\
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


@require_context
def alert_rule_get(context, volume_id, expected_attrs=None):
    return _alert_rule_get(context, volume_id)


@require_context
def alert_rule_create(context, values):
    alert_rule_ref = models.AlertRule()
    alert_rule_ref.update(values)
    session = get_session()
    with session.begin():
        alert_rule_ref.save(session)

    return alert_rule_ref


@handle_db_data_error
@require_context
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
    filters['cluster_id'] = context.cluster_id
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
    filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _alert_rule_get_query(context, session)
        process_filters(models.AlertRule)(query, filters)
        return query.count()

###############################


def _disk_load_attr(context, disk, expected_attrs=None, session=None):
    expected_attrs = expected_attrs or []
    if 'node' in expected_attrs:
        disk.node = disk._node
    if "partition_used" in expected_attrs:
        parts = model_query(
            context, models.DiskPartition, session=session
        ).filter_by(status='inuse', disk_id= disk.id)
        disk.partition_used = parts.count()


def _disk_get_query(context, session=None):
    return model_query(
        context, models.Disk, session=session
    ).filter_by(cluster_id=context.cluster_id)


def _disk_get(context, disk_id, session=None):
    result = _disk_get_query(context, session)
    result = result.filter_by(id=disk_id).first()

    if not result:
        raise exception.DiskNotFound(disk_id=disk_id)

    return result


@require_context
def disk_create(context, values):
    disk_ref = models.Disk()
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
        model_query(context, models.disk, session=session).\
            filter_by(id=disk_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def disk_get(context, disk_id, expected_attrs=None):
    disk = _disk_get(context, disk_id)
    _disk_load_attr(context, disk, expected_attrs)
    return disk


@require_context
def disk_get_all(context, marker=None, limit=None, sort_keys=None,
                 sort_dirs=None, filters=None, offset=None,
                 expected_attrs=None):
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
def disk_update(context, disk_id, values):
    session = get_session()
    with session.begin():
        query = _disk_get_query(context, session)
        result = query.filter_by(id=disk_id).update(values)
        if not result:
            raise exception.DiskNotFound(disk_id=disk_id)


###############################


def _disk_partition_get_query(context, session=None):
    return model_query(
        context, models.DiskPartition, session=session
    ).filter_by(cluster_id=context.cluster_id)


def _disk_partition_get(context, disk_part_id, session=None):
    result = _disk_get_query(context, session)
    result = result.filter_by(id=disk_part_id).first()

    if not result:
        raise exception.DiskPartitionNotFound(disk_part_id=disk_part_id)

    return result


@require_context
def disk_partition_create(context, values):
    disk_part_ref = models.DiskPartition()
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
        model_query(context, models.DiskPartition, session=session).\
            filter_by(id=disk_part_id).\
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
    filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _disk_partition_get_query(context, session)
        process_filters(models.DiskPartition)(query, filters)
        return query.count()


@require_context
def disk_partition_update(context, disk_part_id, values):
    session = get_session()
    with session.begin():
        query = _disk_partition_get_query(context, session)
        result = query.filter_by(id=disk_part_id).update(values)
        if not result:
            raise exception.DiskPartitionNotFound(disk_part_id=disk_part_id)


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
    return _alert_group_get(context, alert_group_id)


@require_context
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
def alert_group_update(context, alert_group_id, values):
    session = get_session()
    with session.begin():
        query = _alert_group_get_query(context, session)
        result = query.filter_by(id=alert_group_id).first()
        if not result:
            raise exception.AlertGroupNotFound(alert_group_id=alert_group_id)
        alert_rule_ids = values.pop('alert_rule_ids')
        db_rules = [_alert_rule_get(context, rule_id, session) for rule_id in
                    alert_rule_ids]
        # update relations:alert_rules,email_groups
        result.alert_rules = db_rules
        email_group_ids = values.pop('email_group_ids')
        db_emails = [_email_group_get(context, email_id, session)
                     for email_id in email_group_ids]
        result.email_groups = db_emails
        result.update(values)


@require_context
def alert_group_get_all(context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
    session = get_session()
    filters = filters or {}
    filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.AlertGroup,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


def alert_group_destroy(context, alert_group_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')
                          }
        db_obj = model_query(context, models.AlertGroup, session=session).\
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
    filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _alert_group_get_query(context, session)
        process_filters(models.AlertGroup)(query, filters)
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
    return _email_group_get(context, email_group_id)


@require_context
def email_group_create(context, values):
    email_group_ref = models.EmailGroup()
    email_group_ref.update(values)
    session = get_session()
    with session.begin():
        email_group_ref.save(session)

    return email_group_ref


@handle_db_data_error
@require_context
def email_group_update(context, email_group_id, values):
    session = get_session()
    with session.begin():
        query = _email_group_get_query(context, session)
        result = query.filter_by(id=email_group_id).update(values)
        if not result:
            raise exception.EmailGroupNotFound(email_group_id=email_group_id)


@require_context
def email_group_get_all(context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
    session = get_session()
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.EmailGroup,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


def email_group_destroy(context, email_group_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        db_obj = model_query(context, models.EmailGroup, session=session).\
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
    return _alert_log_get(context, alert_log_id)


@require_context
def alert_log_create(context, values):
    alert_log_ref = models.AlertLog()
    alert_log_ref.update(values)
    session = get_session()
    with session.begin():
        alert_log_ref.save(session)

    return alert_log_ref


@handle_db_data_error
@require_context
def alert_log_update(context, alert_log_id, values):
    session = get_session()
    with session.begin():
        query = _alert_log_get_query(context, session)
        result = query.filter_by(id=alert_log_id).update(values)
        if not result:
            raise exception.AlertLogNotFound(alert_log_id=alert_log_id)


@require_context
def alert_log_get_all(context, marker=None, limit=None, sort_keys=None,
                      sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
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
        return query.all()


@require_context
def alert_log_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _alert_log_get_query(context, session)
        process_filters(models.AlertLog)(query, filters)
        return query.count()


def alert_log_destroy(context, alert_log_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.AlertLog, session=session).\
            filter_by(id=alert_log_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


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
def log_file_create(context, values):
    log_file_ref = models.LogFile()
    log_file_ref.update(values)
    session = get_session()
    with session.begin():
        log_file_ref.save(session)

    return log_file_ref


@handle_db_data_error
@require_context
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
    with session.begin():
        # Generate the query
        query = _generate_paginate_query(
            context, session, models.LogFile,
            marker, limit,
            sort_keys, sort_dirs, filters, offset)
        if query is None:
            return []
        return query.all()


def log_file_destroy(context, log_file_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.LogFile, session=session).\
            filter_by(id=log_file_id).\
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


def _snap_load_attr(snap, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'volume' in expected_attrs:
        snap.volume = snap._volume


@require_context
def volume_snapshot_get(context, volume_snapshot_id, expected_attrs=None):

    session = get_session()
    with session.begin():
        snap = _volume_snapshot_get(context, volume_snapshot_id, session)
        _snap_load_attr(snap, expected_attrs)
    return snap


@require_context
def volume_snapshot_create(context, values):
    volume_snapshot_ref = models.VolumeSnapshot()
    volume_snapshot_ref.update(values)
    session = get_session()
    with session.begin():
        volume_snapshot_ref.save(session)

    return volume_snapshot_ref


@handle_db_data_error
@require_context
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

        expected_attrs = expected_attrs or []
        if 'volume' in expected_attrs:
            for snapshot in volume_snapshots:
                snapshot.volume = snapshot._volume
        return volume_snapshots


def volume_snapshot_destroy(context, volume_snapshot_id):
    session = get_session()
    now = timeutils.utcnow()
    with session.begin():
        updated_values = {'deleted': True,
                          'deleted_at': now,
                          'updated_at': literal_column('updated_at')}
        model_query(context, models.VolumeSnapshot, session=session).\
            filter_by(id=volume_snapshot_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


###############################


def _service_get_query(context, session=None):
    return model_query(
        context, models.Service, session=session
    ).filter_by(cluster_id=context.cluster_id)


def _service_get(context, service_id, session=None):
    result = _service_get_query(context, session)
    result = result.filter_by(id=service_id).first()

    if not result:
        raise exception.ServiceNotFound(service_id=service_id)

    return result


@require_context
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
        model_query(context, models.Service, session=session).\
            filter_by(id=service_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


@require_context
def service_get(context, service_id, expected_attrs=None):
    return _service_get(context, service_id)


@require_context
def service_get_all(context, marker=None, limit=None, sort_keys=None,
                    sort_dirs=None, filters=None, offset=None):
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
        model_query(context, models.CephConfig, session=session).\
            filter_by(id=ceph_config_id).\
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
            raise exception.CephConfigKeyNotFound(group=group, key=key)
        return result


@require_context
def ceph_config_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _ceph_config_get_query(context, session)
        process_filters(models.CephConfig)(query, filters)
        return query.count()


@require_context
def ceph_config_get_all(context, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
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
def crush_rule_create(context, values):
    crush_rule_ref = models.CrushRule()
    crush_rule_ref.update(values)
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
        model_query(context, models.CrushRule, session=session).\
            filter_by(id=crush_rule_id).\
            update(updated_values)
    del updated_values['updated_at']
    return updated_values


def _crush_rule_load_attr(crush_rule, expected_attrs=None):
    expected_attrs = expected_attrs or []
    if 'osds' in expected_attrs:
        crush_rule.osds = [osd for osd in crush_rule._osds]


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
def crush_rule_update(context, crush_rule_id, values):
    session = get_session()
    with session.begin():
        query = _crush_rule_get_query(context, session)
        result = query.filter_by(id=crush_rule_id).update(values)
        if not result:
            raise exception.CrushRuleNotFound(crush_rule_id=crush_rule_id)


###############################


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
    return _action_log_get(context, action_log_id)


@require_context
def action_log_create(context, values):
    action_log_ref = models.ActionLog()
    action_log_ref.update(values)
    session = get_session()
    with session.begin():
        action_log_ref.save(session)

    return action_log_ref


@handle_db_data_error
@require_context
def action_log_update(context, action_log_id, values):
    session = get_session()
    with session.begin():
        query = _action_log_get_query(context, session)
        result = query.filter_by(id=action_log_id).update(values)
        if not result:
            raise exception.ActionLogNotFound(action_log_id=action_log_id)


@require_context
def action_log_get_all(context, marker=None, limit=None, sort_keys=None,
                       sort_dirs=None, filters=None, offset=None):
    filters = filters or {}
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
        return query.all()


@require_context
def action_log_get_count(context, filters=None):
    session = get_session()
    filters = filters or {}
    filters['cluster_id'] = context.cluster_id
    with session.begin():
        # Generate the query
        query = _action_log_get_query(context, session)
        process_filters(models.ActionLog)(query, filters)
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
def user_create(context, values):
    user_ref = models.User()
    user_ref.update(values)
    session = get_session()
    with session.begin():
        user_ref.save(session)

    return user_ref


@handle_db_data_error
@require_context
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
        process_filters(models.User)(query, filters)
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


########################


PAGINATION_HELPERS = {
    models.Volume: (_volume_get_query, _process_volume_filters, _volume_get),
    models.Node: (_node_get_query, process_filters(models.Node), _node_get),
    models.Osd: (_osd_get_query, process_filters(models.Osd), _osd_get),
    models.Cluster: (_cluster_get_query, _process_cluster_filters,
                     _cluster_get),
    models.RPCService: (_rpc_service_get_query,
                        process_filters(models.RPCService), _rpc_service_get),
    models.VolumeAccessPath: (_volume_access_path_get_query,
                              process_filters(models.VolumeAccessPath),
                              _volume_access_path_get),
    models.VolumeGateway: (_volume_gateway_get_query),
    models.VolumeClient: (_volume_client_get_query,
                          process_filters(models.VolumeClient),
                          _volume_client_get),
    models.VolumeClientGroup: (_volume_client_group_get_query,
                               process_filters(models.VolumeClientGroup),
                               _volume_client_group_get),
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
                  process_filters(models.Datacenter),
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
                       process_filters(models.ActionLog),
                       _action_log_get),
    models.User: (_user_get_query,
                  process_filters(models.User),
                  _user_get),
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
