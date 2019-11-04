#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oslo_versionedobjects import fields

from DSpace import db
from DSpace import exception
from DSpace import objects
from DSpace.objects import base
from DSpace.objects import fields as s_fields


@base.StorObjectRegistry.register
class CrushRule(base.StorPersistentObject, base.StorObject,
                base.StorObjectDictCompat, base.StorComparableObject):
    fields = {
        'id': fields.IntegerField(),
        'rule_id': fields.IntegerField(),
        'rule_name': fields.StringField(),
        'type': fields.StringField(),
        'content': s_fields.DictOfNullableField(),
        'cluster_id': fields.UUIDField(nullable=True),
        'osds': fields.ListOfObjectsField("Osd", nullable=True)
    }

    OPTIONAL_FIELDS = ('osds')

    def create(self):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.stor_obj_get_changes()

        db_crush_rule = db.crush_rule_create(self._context, updates)
        self._from_db_object(self._context, self, db_crush_rule)

    def save(self):
        updates = self.stor_obj_get_changes()
        if updates:
            db.crush_rule_update(self._context, self.id, updates)

        self.obj_reset_changes()

    def destroy(self):
        updated_values = db.crush_rule_destroy(self._context, self.id)
        self.update(updated_values)
        self.obj_reset_changes(updated_values.keys())

    @classmethod
    def _from_db_object(cls, context, obj, db_obj, expected_attrs=None):
        expected_attrs = expected_attrs or []
        if 'osds' in expected_attrs:
            osds = db_obj.get('osds', [])
            obj.osds = [objects.Osd._from_db_object(
                context, objects.Osd(context), osd
            ) for osd in osds]

        return super(CrushRule, cls)._from_db_object(context, obj, db_obj)


@base.StorObjectRegistry.register
class CrushRuleList(base.ObjectListBase, base.StorObject):

    fields = {
        'objects': fields.ListOfObjectsField('CrushRule'),
    }

    @classmethod
    def get_all(cls, context, filters=None, marker=None, limit=None,
                offset=None, sort_keys=None, sort_dirs=None,
                expected_attrs=None):
        crush_rules = db.crush_rule_get_all(context, filters, marker, limit,
                                            offset, sort_keys, sort_dirs,
                                            expected_attrs)
        return base.obj_make_list(context, cls(context), objects.CrushRule,
                                  crush_rules, expected_attrs=expected_attrs)
