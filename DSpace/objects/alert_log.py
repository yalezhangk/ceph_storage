from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class AlertLog(base.StorPersistentObject, base.StorObject,
               base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'readed': fields.BooleanField(),
        'resource_type': fields.StringField(),
        'level': fields.StringField(),
        'alert_value': fields.StringField(),
        'resource_id': fields.StringField(),
        'resource_name': fields.StringField(),
        'alert_rule_id': fields.IntegerField(),
        'cluster_id': fields.UUIDField(),
        'alert_rule': fields.ObjectField("AlertRule", nullable=True),
    }

    OPTIONAL_FIELDS = ('alert_rule',)

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_cluster = db.alert_log_create(self._context, updates)
        self._from_db_object(self._context, self, db_cluster)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.alert_log_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.alert_log_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []

        if 'alert_rule' in expected_attrs:
            alert_rule = db_obj.get('alert_rule', None)
            if alert_rule:
                obj.alert_rule = objects.AlertRule._from_db_object(
                    context, objects.AlertRule(context), alert_rule)
            else:
                obj.alert_rule = None

        return super(AlertLog, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class AlertLogList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('AlertLog'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        alert_logs = db.alert_log_get_all(context, marker, limit,
                                          sort_keys, sort_dirs, filters,
                                          offset, expected_attrs)
        return base.obj_make_list(context, cls(context), objects.AlertLog,
                                  alert_logs, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.alert_log_get_count(context, filters)
        return count

    @classmethod
    def update(cls, context, filters=None, updates=None):
        result = db.alert_log_batch_update(context, filters, updates)
        return result
