from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class ObjectLifecycle(base.StorPersistentObject, base.StorObject,
                      base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'name': fields.StringField(),
        'enabled': fields.BooleanField(),
        'target': fields.StringField(nullable=True),
        'policy': s_fields.DictOfNullableField(),
        'cluster_id': fields.UUIDField(),
        'bucket_id': fields.IntegerField(),
        'bucket': fields.ObjectField("ObjectBucket", nullable=True),
    }

    OPTIONAL_FIELDS = ('bucket',)

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_lifecycle = db.object_lifecycle_create(self._context, updates)
        self._from_db_object(self._context, self, db_lifecycle)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.object_lifecycle_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.object_lifecycle_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'bucket' in expected_attrs:
            bucket = db_obj.get('bucket', None)
            obj.bucket = objects.ObjectBucket._from_db_object(
                context, objects.ObjectBucket(context), bucket)

        return super(ObjectLifecycle, cls)._from_db_object(
            context, obj, db_obj)


@base.StorObjectRegistry.register
class ObjectLifecycleList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('ObjectLifecycle'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        object_lifecycles = db.object_lifecycle_get_all(
            context, marker=marker, limit=limit,
            sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
            offset=offset, expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context), objects.ObjectLifecycle, object_lifecycles,
            expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.object_lifecycle_get_count(context, filters)
        return count
