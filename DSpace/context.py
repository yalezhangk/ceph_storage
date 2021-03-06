import uuid

from oslo_context import context

from DSpace.i18n import _


class RequestContext(context.RequestContext):
    def __init__(self, read_deleted='no', cluster_id=None, client_ip=None,
                 ws_ip=None, **kwargs):
        request_id = kwargs.pop("request_id", None)
        if not request_id:
            request_id = str(uuid.uuid4())
        self.read_deleted = read_deleted
        self.cluster_id = cluster_id
        self.client_ip = client_ip
        self.ws_ip = ws_ip
        super(RequestContext, self).__init__(
            request_id=request_id, **kwargs)

    def to_dict(self):
        res = super(RequestContext, self).to_dict()
        res.update({
            "user_id": self.user_id,
            "read_deleted": self.read_deleted,
            "cluster_id": self.cluster_id,
            "client_ip": self.client_ip,
            "ws_ip": self.ws_ip,
        })
        return res

    @classmethod
    def from_dict(cls, values):
        return cls(user_id=values['user_id'],
                   read_deleted=values['read_deleted'],
                   cluster_id=values['cluster_id'],
                   client_ip=values.get('client_ip'),
                   is_admin=values['is_admin'],
                   ws_ip=values['ws_ip'],
                   request_id=values['request_id'])

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


def get_context(cluster_id=None, user_id=None):
    user_id = user_id or "admin"
    ctxt = RequestContext(user_id=user_id, is_admin=False,
                          cluster_id=cluster_id)
    return ctxt


def get_admin_context():
    ctxt = RequestContext(user_id='admin', is_admin=True, cluster_id=None)
    return ctxt
