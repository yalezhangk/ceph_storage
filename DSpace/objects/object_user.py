from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class ObjectUser(base.StorPersistentObject, base.StorObject,
                 base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'uid': fields.StringField(),
        'email': fields.StringField(nullable=True),
        'display_name': fields.StringField(),
        'status': s_fields.ObjectUserStatusField(),
        'suspended': fields.BooleanField(),
        'op_mask': fields.StringField(),
        'max_buckets': fields.IntegerField(),
        'bucket_quota_max_size': fields.IntegerField(),
        'bucket_quota_max_objects': fields.IntegerField(),
        'user_quota_max_size': fields.IntegerField(),
        'user_quota_max_objects': fields.IntegerField(),
        'capabilities': fields.StringField(nullable=True),
        'cluster_id': fields.StringField(),
        'is_admin': fields.BooleanField(),
        'description': fields.StringField(nullable=True),
        'access_keys': fields.ListOfObjectsField('ObjectAccessKey',
                                                 nullable=True),
        'metrics': s_fields.DictOfNullableField(nullable=True),

    }

    OPTIONAL_FIELDS = ('access_keys', 'metrics')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()
        db_object_user = db.object_user_create(self._context, updates)
        self._from_db_object(self._context, self, db_object_user)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.object_user_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.object_user_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        obj.metrics = {}
        expected_attrs = expected_attrs or []
        if 'access_keys' in expected_attrs:
            access_keys = db_obj.get('access_keys', [])
            obj.access_keys = [objects.ObjectAccessKey._from_db_object(
                context, objects.ObjectAccessKey(context), access_key
            ) for access_key in access_keys]
        return super(ObjectUser, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class ObjectUserList(base.ObjectListBase, base.StorObject):
    fields = {
        'objects': fields.ListOfObjectsField('ObjectUser'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        users = db.object_user_get_all(context, filters, marker, limit, offset,
                                       sort_keys, sort_dirs,
                                       expected_attrs=expected_attrs)
        return base.obj_make_list(context, cls(context), objects.ObjectUser,
                                  users, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.object_user_get_count(context, filters)
        return count
