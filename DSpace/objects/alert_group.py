from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class AlertGroup(base.StorPersistentObject, base.StorObject,
                 base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'alert_rule_ids': fields.ListOfIntegersField(nullable=True),
        'email_group_ids': fields.ListOfIntegersField(nullable=True),
        'cluster_id': fields.UUIDField(),
        'alert_rules': fields.ListOfObjectsField('AlertRule', nullable=True),
        'email_groups': fields.ListOfObjectsField('EmailGroup', nullable=True)
    }

    OPTIONAL_FIELDS = ('alert_rules', 'email_groups')

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

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []

        if 'alert_rules' in expected_attrs:
            alert_rules = db_obj.get('alert_rules', [])
            obj.alert_rules = [objects.AlertRule._from_db_object(
                context, objects.AlertRule(context), alert_rule
            ) for alert_rule in alert_rules]

        if 'email_groups' in expected_attrs:
            email_groups = db_obj.get('email_groups', [])
            obj.email_groups = [objects.EmailGroup._from_db_object(
                context, objects.EmailGroup(context), email_group
            ) for email_group in email_groups]

        return super(AlertGroup, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class AlertGroupList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('AlertGroup'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        alert_groups = db.alert_group_get_all(context, marker, limit,
                                              sort_keys, sort_dirs, filters,
                                              offset, expected_attrs)
        return base.obj_make_list(context, cls(context), objects.AlertGroup,
                                  alert_groups, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.alert_group_get_count(context, filters)
        return count
