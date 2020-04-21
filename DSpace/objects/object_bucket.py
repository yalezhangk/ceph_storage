from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class ObjectBucket(base.StorPersistentObject, base.StorObject,
                   base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'status': s_fields.BucketStatusField(nullable=True),
        'bucket_id': fields.StringField(nullable=True),
        'policy_id': fields.IntegerField(),
        'owner_id': fields.IntegerField(),
        'shards': fields.IntegerField(),
        'versioned': fields.BooleanField(),
        'owner_permission': fields.StringField(),
        'auth_user_permission': fields.StringField(),
        'all_user_permission': fields.StringField(),
        'quota_max_size': fields.IntegerField(),
        'quota_max_objects': fields.IntegerField(),
        'cluster_id': fields.UUIDField(),
        'policy': fields.ObjectField("ObjectPolicy", nullable=True),
        'owner': fields.ObjectField("ObjectUser", nullable=True),
        'lifecycles': fields.ListOfObjectsField("ObjectLifecycle",
                                                nullable=True),
        'used_capacity_quota': fields.IntegerField(nullable=True),
        'used_object_quota': fields.IntegerField(nullable=True),
        'metrics': s_fields.DictOfNullableField(nullable=True),
    }

    OPTIONAL_FIELDS = ('owner', 'policy', 'lifecycles',
                       'used_capacity_quota', 'used_object_quota',
                       'metrics')

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
        if 'owner' in expected_attrs:
            owner = db_obj.get('owner', None)
            obj.owner = objects.ObjectUser._from_db_object(
                context, objects.ObjectUser(context), owner)

        if 'policy' in expected_attrs:
            policy = db_obj.get('policy', None)
            obj.policy = objects.ObjectPolicy._from_db_object(
                    context, objects.ObjectPolicy(context), policy)

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
