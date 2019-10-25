from oslo_versionedobjects import fields

from t2stor import db
from t2stor import exception
from t2stor import objects
from t2stor.objects import base


@base.StorObjectRegistry.register
class LogFile(base.StorPersistentObject, base.StorObject,
              base.StorObjectDictCompat, base.StorComparableObject):

    fields = {
        'id': fields.IntegerField(),
        'node_id': fields.IntegerField(),
        'service_type': fields.StringField(),
        'directory': fields.StringField(),
        'filename': fields.StringField(),
        'filesize': fields.IntegerField(),
        'cluster_id': fields.UUIDField(),
    }

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_cluster = db.log_file_create(self._context, updates)
        self._from_db_object(self._context, self, db_cluster)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.log_file_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.log_file_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())


@base.StorObjectRegistry.register
class LogFileList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('LogFile'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None):
        log_files = db.alert_group_get_all(context, marker, limit,
                                           sort_keys, sort_dirs, filters,
                                           offset)
        expected_attrs = LogFile._get_expected_attrs(context)
        return base.obj_make_list(context, cls(context), objects.LogFile,
                                  log_files, expected_attrs=expected_attrs)
