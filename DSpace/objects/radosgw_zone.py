from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base


@base.StorObjectRegistry.register
class RadosgwZone(base.StorPersistentObject, base.StorObject,
                  base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'description': fields.StringField(nullable=True),
        'name': fields.StringField(),
        'zone_id': fields.StringField(nullable=True),
        'zonegroup': fields.StringField(nullable=True),
        'realm': fields.StringField(nullable=True),
        'cluster_id': fields.StringField(),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_radosgw_zone = db.radosgw_zone_create(self._context, updates)
        self._from_db_object(self._context, self, db_radosgw_zone)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.radosgw_zone_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.radosgw_zone_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class RadosgwZoneList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('RadosgwZone'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        radosgw_zoness = db.radosgw_zone_get_all(
            context, filters, marker, limit, offset, sort_keys, sort_dirs)
        return base.obj_make_list(context, cls(context), objects.RadosgwZone,
                                  radosgw_zoness)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.radosgw_zone_get_count(context, filters)
        return count
