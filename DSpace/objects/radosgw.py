from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class Radosgw(base.StorPersistentObject, base.StorObject,
              base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'description': fields.StringField(nullable=True),
        'name': fields.StringField(),
        'display_name': fields.StringField(nullable=True),
        'status': s_fields.RadosgwStatusField(),
        'ip_address': fields.IPAddressField(),
        'port': fields.IntegerField(),
        'zone': fields.StringField(nullable=True),
        'node_id': fields.IntegerField(),
        'cluster_id': fields.StringField(),
        'node': fields.ObjectField("Node", nullable=True),
    }

    OPTIONAL_FIELDS = ('node', )

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_radosgw = db.radosgw_create(self._context, updates)
        self._from_db_object(self._context, self, db_radosgw)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.radosgw_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.radosgw_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'node' in expected_attrs:
            node = db_obj.get('node', None)
            obj.node = objects.Node._from_db_object(
                context, objects.Node(context), node
            )
        return super(Radosgw, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class RadosgwList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('Radosgw'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        radosgws = db.radosgw_get_all(
            context, filters, marker, limit, offset, sort_keys, sort_dirs,
            expected_attrs=expected_attrs)
        return base.obj_make_list(context, cls(context), objects.Radosgw,
                                  radosgws, expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.radosgw_get_count(context, filters)
        return count
