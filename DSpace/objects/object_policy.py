from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class ObjectPolicy(base.StorPersistentObject, base.StorObject,
                   base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'description': fields.StringField(nullable=True),
        'default': fields.BooleanField(),
        'index_pool_id': fields.IntegerField(),
        'data_pool_id': fields.IntegerField(),
        'compression': fields.StringField(nullable=True),
        'index_type': fields.IntegerField(),
        'cluster_id': fields.UUIDField(),
        'index_pool': fields.ObjectField("Pool", nullable=True),
        'data_pool': fields.ObjectField("Pool", nullable=True),
        'buckets': fields.ListOfObjectsField("ObjectBucket", nullable=True),
    }

    OPTIONAL_FIELDS = ('index_pool', 'data_pool', 'buckets')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_object_policy = db.object_policy_create(self._context, updates)
        self._from_db_object(self._context, self, db_object_policy)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.object_policy_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.object_policy_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'index_pool' in expected_attrs:
            index_pool = db_obj.get('index_pool', None)
            obj.index_pool = objects.Pool._from_db_object(
                context, objects.Pool(context), index_pool)

        if 'data_pool' in expected_attrs:
            data_pool = db_obj.get('data_pool', None)
            obj.data_pool = objects.Pool._from_db_object(
                context, objects.Pool(context), data_pool)

        if 'buckets' in expected_attrs:
            buckets = db_obj.get('buckets', [])
            obj.buckets = [objects.ObjectBucket._from_db_object(
                context, objects.ObjectBucket(context), bucket
            ) for bucket in buckets]

        return super(ObjectPolicy, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class ObjectPolicyList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('ObjectPolicy'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        object_policies = db.object_policy_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context), objects.ObjectPolicy, object_policies,
            expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.object_policy_get_count(context, filters)
        return count
