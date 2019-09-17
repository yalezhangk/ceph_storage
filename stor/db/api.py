from oslo_config import cfg
from oslo_db import api as oslo_db_api
from oslo_db import options as db_options

from stor.common import constants
from stor.i18n import _


CONF = cfg.CONF
db_options.set_defaults(CONF)

_BACKEND_MAPPING = {'sqlalchemy': 'stor.db.sqlalchemy.api'}


IMPL = oslo_db_api.DBAPI.from_config(conf=CONF,
                                     backend_mapping=_BACKEND_MAPPING,
                                     lazy=True)

# The maximum value a signed INT type may have
MAX_INT = constants.DB_MAX_INT


###################

def dispose_engine():
    """Force the engine to establish new connections."""

    # FIXME(jdg): When using sqlite if we do the dispose
    # we seem to lose our DB here.  Adding this check
    # means we don't do the dispose, but we keep our sqlite DB
    # This likely isn't the best way to handle this

    if 'sqlite' not in IMPL.get_engine().name:
        return IMPL.dispose_engine()
    else:
        return


###############


def volume_create(context, values):
    """Create a volume from the values dictionary."""
    return IMPL.volume_create(context, values)


def volume_destroy(context, volume_id):
    """Destroy the volume or raise if it does not exist."""
    return IMPL.volume_destroy(context, volume_id)


def volume_get(context, volume_id):
    """Get a volume or raise if it does not exist."""
    return IMPL.volume_get(context, volume_id)


def volume_get_all(context, marker=None, limit=None, sort_keys=None,
                   sort_dirs=None, filters=None, offset=None):
    """Get all volumes."""
    return IMPL.volume_get_all(context, marker, limit, sort_keys=sort_keys,
                               sort_dirs=sort_dirs, filters=filters,
                               offset=offset)


def volume_update(context, volume_id, values):
    """Set the given properties on a volume and update it.

    Raises NotFound if volume does not exist.

    """
    return IMPL.volume_update(context, volume_id, values)


def volumes_update(context, values_list):
    """Set the given properties on a list of volumes and update them.

    Raises NotFound if a volume does not exist.
    """
    return IMPL.volumes_update(context, values_list)


###################


def resource_exists(context, model, resource_id):
    return IMPL.resource_exists(context, model, resource_id)


def get_model_for_versioned_object(versioned_object):
    return IMPL.get_model_for_versioned_object(versioned_object)


def get_by_id(context, model, id, *args, **kwargs):
    return IMPL.get_by_id(context, model, id, *args, **kwargs)


class Condition(object):
    """Class for normal condition values for conditional_update."""
    def __init__(self, value, field=None):
        self.value = value
        # Field is optional and can be passed when getting the filter
        self.field = field

    def get_filter(self, model, field=None):
        return IMPL.condition_db_filter(model, self._get_field(field),
                                        self.value)

    def _get_field(self, field=None):
        # We must have a defined field on initialization or when called
        field = field or self.field
        if not field:
            raise ValueError(_('Condition has no field.'))
        return field

###################


class Not(Condition):
    """Class for negated condition values for conditional_update.

    By default NULL values will be treated like Python treats None instead of
    how SQL treats it.

    So for example when values are (1, 2) it will evaluate to True when we have
    value 3 or NULL, instead of only with 3 like SQL does.
    """
    def __init__(self, value, field=None, auto_none=True):
        super(Not, self).__init__(value, field)
        self.auto_none = auto_none

    def get_filter(self, model, field=None):
        # If implementation has a specific method use it
        if hasattr(IMPL, 'condition_not_db_filter'):
            return IMPL.condition_not_db_filter(model, self._get_field(field),
                                                self.value, self.auto_none)

        # Otherwise non negated object must adming ~ operator for not
        return ~super(Not, self).get_filter(model, field)


class Case(object):
    """Class for conditional value selection for conditional_update."""
    def __init__(self, whens, value=None, else_=None):
        self.whens = whens
        self.value = value
        self.else_ = else_


def is_orm_value(obj):
    """Check if object is an ORM field."""
    return IMPL.is_orm_value(obj)


def conditional_update(context, model, values, expected_values, filters=(),
                       include_deleted='no', project_only=False, order=None):
    """Compare-and-swap conditional update.

    Update will only occur in the DB if conditions are met.

    We have 4 different condition types we can use in expected_values:
     - Equality:  {'status': 'available'}
     - Inequality: {'status': vol_obj.Not('deleting')}
     - In range: {'status': ['available', 'error']
     - Not in range: {'status': vol_obj.Not(['in-use', 'attaching'])

    Method accepts additional filters, which are basically anything that can be
    passed to a sqlalchemy query's filter method, for example:

    .. code-block:: python

     [~sql.exists().where(models.Volume.id == models.Snapshot.volume_id)]

    We can select values based on conditions using Case objects in the 'values'
    argument. For example:

    .. code-block:: python

     has_snapshot_filter = sql.exists().where(
         models.Snapshot.volume_id == models.Volume.id)
     case_values = db.Case([(has_snapshot_filter, 'has-snapshot')],
                           else_='no-snapshot')
     db.conditional_update(context, models.Volume, {'status': case_values},
                           {'status': 'available'})

    And we can use DB fields for example to store previous status in the
    corresponding field even though we don't know which value is in the db from
    those we allowed:

    .. code-block:: python

     db.conditional_update(context, models.Volume,
                           {'status': 'deleting',
                            'previous_status': models.Volume.status},
                           {'status': ('available', 'error')})

    :param values: Dictionary of key-values to update in the DB.
    :param expected_values: Dictionary of conditions that must be met for the
                            update to be executed.
    :param filters: Iterable with additional filters.
    :param include_deleted: Should the update include deleted items, this is
                            equivalent to read_deleted.
    :param project_only: Should the query be limited to context's project.
    :param order: Specific order of fields in which to update the values
    :returns: Number of db rows that were updated.
    """
    return IMPL.conditional_update(context, model, values, expected_values,
                                   filters, include_deleted, project_only,
                                   order)
