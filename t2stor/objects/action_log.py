from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


@base.StorObjectRegistry.register
class ActionLog(base.StorPersistentObject, base.StorObject,
                base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'begin_time': fields.DateTimeField(),
        'finish_time': fields.DateTimeField(),
        'client_ip': fields.StringField(),
        'user_id': fields.StringField(),
        'action': fields.StringField(),
        'resource_id': fields.StringField(nullable=True),
        'resource_name': fields.StringField(),
        'resource_type': fields.StringField(),
        'resource_data': fields.StringField(),
        'status': fields.StringField(),
        'cluster_id': fields.UUIDField(),
    }

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


@base.StorObjectRegistry.register
class ActionLogList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('ActionLog'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        action_logs = db.action_log_get_all(context, marker, limit,
                                            sort_keys, sort_dirs, filters,
                                            offset)
        return base.obj_make_list(context, cls(context), objects.ActionLog,
                                  action_logs)