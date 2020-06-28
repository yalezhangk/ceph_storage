import json
import logging

from oslo_utils import timeutils
from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects.base import StorObject

logger = logging.getLogger(__name__)


@base.StorObjectRegistry.register
class ActionLog(base.StorPersistentObject, base.StorObject,
                base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'begin_time': fields.DateTimeField(),
        'finish_time': fields.DateTimeField(nullable=True),
        'client_ip': fields.StringField(nullable=True),
        'user_id': fields.StringField(),
        'action': fields.StringField(nullable=True),
        'resource_id': fields.StringField(nullable=True),
        'resource_name': fields.StringField(nullable=True),
        'resource_type': fields.StringField(nullable=True),
        'before_data': fields.StringField(nullable=True),
        'after_data': fields.StringField(nullable=True),
        'diff_data': fields.StringField(nullable=True),
        'status': fields.StringField(),
        'err_msg': fields.StringField(nullable=True),
        'cluster_id': fields.UUIDField(nullable=True),
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

    def finish_action(self, resource_id=None,
                      resource_name=None, after_obj=None, status=None,
                      action=None, err_msg=None, diff_data=None,
                      *args, **kwargs):
        if isinstance(after_obj, StorObject):
            after_data = json.dumps(after_obj.to_dict())
        else:
            after_data = json.dumps(after_obj)
        finish_data = {
            'resource_id': resource_id,
            'resource_name': resource_name,
            'after_data': (after_data if after_data else None),
            'status': 'success',
            'finish_time': timeutils.utcnow(),
            'err_msg': err_msg,
            'diff_data': diff_data
        }
        if action:
            finish_data.update({'action': action})
        if status:
            if status in ['active', 'success', 'available', 'deleted']:
                finish_data.update({'status': 'success'})
            else:
                finish_data.update({'status': 'fail'})
        self.update(finish_data)
        logger.debug('finish action, resource_name:%s, action:%s, status:%s',
                     resource_name, action, finish_data['status'])
        self.save()


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
