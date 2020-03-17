from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class ObjectBucket(base.StorPersistentObject, base.StorObject,
                   base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'status': fields.StringField(),
        'bucket_id': fields.IntegerField(),
        'policy_id': fields.IntegerField(),
        'owner_id': fields.IntegerField(),
        'shards': fields.IntegerField(),
        'versioned': fields.BooleanField(),
        'owner_permission': fields.StringField(),
        'auth_user_permission': fields.StringField(),
        'quota_mar_size': fields.IntegerField(),
        'quota_mar_objects': fields.IntegerField(),
        'cluster_id': fields.UUIDField(),
        'own_user': fields.ObjectField("ObjectUser", nullable=True),
        'lifecycles': fields.ListOfObjectsField("ObjectLifecycle",
                                                nullable=True),
    }

    OPTIONAL_FIELDS = ('own_user', 'lifecycles')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_object_policy = db.object_bucket_create(self._context, updates)
        self._from_db_object(self._context, self, db_object_policy)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.object_bucket_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.object_bucket_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'own_user' in expected_attrs:
            own_user = db_obj.get('own_user', None)
            obj.own_user = objects.ObjectUser._from_db_object(
                context, objects.ObjectUser(context), own_user)

        if 'lifecycles' in expected_attrs:
            lifecycles = db_obj.get('lifecycles', [])
            obj.lifecycles = [objects.ObjectLifecycle._from_db_object(
                context, objects.ObjectLifecycle(context), lifecycle
            ) for lifecycle in lifecycles]

        return super(ObjectBucket, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class ObjectBucketList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('ObjectBucket'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        object_policies = db.object_bucket_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context), objects.ObjectBucket, object_policies,
            expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.object_bucket_get_count(context, filters)
        return count
