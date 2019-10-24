from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


@base.StorObjectRegistry.register
class AlertGroup(base.StorPersistentObject, base.StorObject,
                 base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'alert_rule_ids': fields.ListOfIntegersField(nullable=True),
        'email_group_ids': fields.ListOfIntegersField(nullable=True),
        'cluster_id': fields.StringField(),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_cluster = db.alert_group_create(self._context, updates)
        self._from_db_object(self._context, self, db_cluster)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.alert_group_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.alert_group_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class AlertGroupList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('AlertGroup'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        alert_groups = db.alert_group_get_all(context, marker, limit,
                                              sort_keys, sort_dirs, filters,
                                              offset)
        expected_attrs = AlertGroup._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context), objects.AlertGroup,
                                  alert_groups, expected_attrs=expected_attrs)
