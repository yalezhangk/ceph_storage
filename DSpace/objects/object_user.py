from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class ObjectUser(base.StorPersistentObject, base.StorObject,
                 base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'uid': fields.StringField(),
        'email': fields.StringField(),
        'display_name': fields.StringField(),
        'status': fields.StringField(),
        'suspended': fields.BooleanField(),
        'op_mask': fields.StringField(),
        'max_bucket': fields.IntegerField(),
        'bucket_quota_max_size': fields.IntegerField(),
        'bucket_quota_max_objects': fields.IntegerField(),
        'user_quota_max_size': fields.IntegerField(),
        'user_quota_max_objects': fields.IntegerField(),
        'description': fields.StringField(),
        'cluster_id': fields.UUIDField(),
        'buckets': fields.ListOfObjectsField("ObjectBucket", nullable=True),
        'access_keys': fields.ListOfObjectsField("ObjectAccessKey",
                                                 nullable=True),
    }

    OPTIONAL_FIELDS = ('buckets', 'access_keys')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_object_policy = db.object_user_create(self._context, updates)
        self._from_db_object(self._context, self, db_object_policy)

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
        expected_attrs = expected_attrs or []

        if 'buckets' in expected_attrs:
            buckets = db_obj.get('buckets', [])
            obj.buckets = [objects.ObjectBucket._from_db_object(
                context, objects.ObjectBucket(context), bucket
            ) for bucket in buckets]

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
        object_policies = db.object_user_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context), objects.ObjectUser, object_policies,
            expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.object_user_get_count(context, filters)
        return count
