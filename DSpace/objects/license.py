from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class License(base.StorPersistentObject, base.StorObject,
              base.StorObjectDictCompat, base.StorComparableObject):
    fields = {
        'id': fields.IntegerField(),
        'content': fields.StringField(),
        'status': fields.StringField()
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()
        db_datacenter = db.license_create(self._context, updates)
        self._from_db_object(self._context, self, db_datacenter)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.license_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        pass


@base.StorObjectRegistry.register
class LicenseList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('License'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        licenses = db.license_get_all(context, marker, limit,
                                      sort_keys, sort_dirs, filters, offset)
        return base.obj_make_list(context, cls(context), objects.License,
                                  licenses)
