from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


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
    }

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
        expected_attrs = AlertRule._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context), objects.AlertRule,
                                  alert_rules, expected_attrs=expected_attrs)
