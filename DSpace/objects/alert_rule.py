from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class AlertRule(base.StorPersistentObject, base.StorObject,
                base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'resource_type': fields.StringField(),
        'type': fields.StringField(),
        'trigger_value': fields.StringField(),
        'level': fields.StringField(),
        'trigger_period': fields.StringField(),
        'enabled': fields.BooleanField(),
        'cluster_id': fields.UUIDField(),
        'alert_groups': fields.ListOfObjectsField('AlertGroup', nullable=True)
    }

    OPTIONAL_FIELDS = ('alert_groups',)

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_cluster = db.alert_rule_create(self._context, updates)
        self._from_db_object(self._context, self, db_cluster)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.alert_rule_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.alert_rule_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []

        if 'alert_groups' in expected_attrs:
            alert_groups = db_obj.get('alert_groups', [])
            obj.alert_groups = [objects.AlertGroup._from_db_object(
                context, objects.AlertGroup(context), alert_group
            ) for alert_group in alert_groups]

        return super(AlertRule, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class AlertRuleList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('AlertRule'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        alert_rules = db.alert_rule_get_all(context, marker, limit, sort_keys,
                                            sort_dirs, filters, offset)
        return base.obj_make_list(context, cls(context), objects.AlertRule,
                                  alert_rules)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.alert_rule_get_count(context, filters)
        return count
