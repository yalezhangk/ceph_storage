from stor.i18n import _


class RequestContext(object):
    def __init__(self, user_id=None, project_id=None, is_admin=False,
                 read_deleted='no'):
        self.user_id = user_id
        self.project_id = project_id
        self.is_admin = is_admin
        self.read_deleted = read_deleted

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "project_id": self.project_id,
            "read_deleted": self.read_deleted,
            "is_admin": self.is_admin,
        }

    @classmethod
    def from_dict(cls, values):
        return cls(user_id=values['user_id'],
                   project_id=values['project_id'],
                   read_deleted=values['read_deleted'],
                   is_admin=values['is_admin'])

    def _get_read_deleted(self):
        return self._read_deleted

    def _set_read_deleted(self, read_deleted):
        if read_deleted not in ('no', 'yes', 'only'):
            raise ValueError(_("read_deleted can only be one of 'no', "
                               "'yes' or 'only', not %r") % read_deleted)
        self._read_deleted = read_deleted

    def _del_read_deleted(self):
        del self._read_deleted

    read_deleted = property(_get_read_deleted, _set_read_deleted,
                            _del_read_deleted)
