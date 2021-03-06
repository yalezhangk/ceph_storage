from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class ObjectAccessKey(base.StorPersistentObject, base.StorObject,
                      base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'obj_user_id': fields.IntegerField(),
        'access_key': fields.StringField(),
        'secret_key': fields.StringField(),
        'type': fields.StringField(),
        'description': fields.StringField(nullable=True),
        'cluster_id': fields.UUIDField(),
        'obj_user': fields.ObjectField("ObjectUser", nullable=True),
    }

    OPTIONAL_FIELDS = ('obj_user',)

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_object_access_key = db.object_access_key_create(self._context,
                                                           updates)
        self._from_db_object(self._context, self, db_object_access_key)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.object_access_key_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.object_access_key_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class ObjectAccessKeyList(base.ObjectListBase, base.StorObject):
    fields = {
        'objects': fields.ListOfObjectsField('ObjectAccessKey'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        access = db.object_access_key_get_all(context, filters, marker, limit,
                                              offset,
                                              sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context),
                                  objects.ObjectAccessKey,
                                  access)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.object_access_key_get_count(context, filters)
        return count
