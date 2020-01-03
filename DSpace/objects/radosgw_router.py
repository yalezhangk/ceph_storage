from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class RadosgwRouter(base.StorPersistentObject, base.StorObject,
                    base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'description': fields.StringField(nullable=True),
        'name': fields.StringField(),
        'status': s_fields.RadosgwRouterStatusField(),
        'virtual_ip': fields.IPAddressField(nullable=True),
        'port': fields.IntegerField(nullable=True),
        'https_port': fields.IntegerField(nullable=True),
        'virtual_router_id': fields.IntegerField(nullable=True),
        'cluster_id': fields.StringField(),
        'nodes': fields.StringField(),
        'radosgws': fields.ListOfObjectsField("Radosgw", nullable=True),
        'router_services': fields.ListOfObjectsField("RouterService",
                                                     nullable=True),
    }

    OPTIONAL_FIELDS = ('radosgws', 'router_services')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_radosgw_router = db.radosgw_router_create(self._context, updates)
        self._from_db_object(self._context, self, db_radosgw_router)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.radosgw_router_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.radosgw_router_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'radosgws' in expected_attrs:
            radosgws = db_obj.get('radosgws', [])
            obj.radosgws = [objects.Radosgw._from_db_object(
                context, objects.Radosgw(context), radosgw
            ) for radosgw in radosgws]
        if 'router_services' in expected_attrs:
            router_services = db_obj.get('router_services', [])
            obj.router_services = [objects.RouterService._from_db_object(
                context, objects.RouterService(context), service
            ) for service in router_services]
        return super(RadosgwRouter, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class RadosgwRouterList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('RadosgwRouter'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        radosgw_routers = db.radosgw_router_get_all(
            context, filters, marker, limit, offset, sort_keys, sort_dirs,
            expected_attrs=expected_attrs)
        return base.obj_make_list(
            context, cls(context), objects.RadosgwRouter, radosgw_routers,
            expected_attrs=expected_attrs)

    @classmethod
    def get_count(cls, context, filters=None):
        count = db.radosgw_router_get_count(context, filters)
        return count
