from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class ActionLog(base.StorPersistentObject, base.StorObject,
                base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'begin_time': fields.DateTimeField(),
        'finish_time': fields.DateTimeField(nullable=True),
        'client_ip': fields.StringField(),
        'user_id': fields.StringField(),
        'action': fields.StringField(),
        'resource_id': fields.StringField(nullable=True),
        'resource_name': fields.StringField(nullable=True),
        'resource_type': fields.StringField(nullable=True),
        'resource_data': fields.StringField(nullable=True),
        'status': fields.StringField(),
        'err_msg': fields.StringField(nullable=True),
        'cluster_id': fields.UUIDField(),
        'user': fields.ObjectField('User', nullable=True)
    }

    OPTIONAL_FIELDS = ('user',)

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_action_log = db.action_log_create(self._context, updates)
        self._from_db_object(self._context, self, db_action_log)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.action_log_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.action_log_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []

        if 'user' in expected_attrs:
            user = db_obj.get('user', [])
            obj.user = objects.User._from_db_object(
                context, objects.User(context), user)
        return super(ActionLog, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class ActionLogList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('ActionLog'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        action_logs = db.action_log_get_all(context, marker, limit,
                                            sort_keys, sort_dirs, filters,
                                            offset, expected_attrs)
        return base.obj_make_list(context, cls(context), objects.ActionLog,
                                  action_logs, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.action_log_get_count(context, filters)
        return count
