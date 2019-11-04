import six
from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler

logger = logging.getLogger(__name__)


class AlertLogHandler(AdminBaseHandler):
    def alert_log_get_all(self, ctxt, marker=None, limit=None,
                          sort_keys=None, sort_dirs=None, filters=None,
                          offset=None):
        return objects.AlertLogList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

    def alert_log_get_count(self, ctxt, filters=None):
        return objects.AlertLogList.get_count(
            ctxt, filters=filters)

    def alert_log_create(self, ctxt, data):
        data.update({'cluster_id': ctxt.cluster_id})
        alert_log = objects.AlertLog(ctxt, **data)
        alert_log.create()
        # TODO send email
        # 根据alert_rule,alert_group,alert_email发送邮件
        return alert_log

    def alert_log_get(self, ctxt, alert_log_id):
        return objects.AlertLog.get_by_id(ctxt, alert_log_id)

    def alert_log_update(self, ctxt, alert_log_id, data):
        alert_log = self.alert_log_get(ctxt, alert_log_id)
        for k, v in six.iteritems(data):
            setattr(alert_log, k, v)
        alert_log.save()
        return alert_log

    def alert_log_delete(self, ctxt, alert_log_id):
        alert_log = self.alert_log_get(ctxt, alert_log_id)
        alert_log.destroy()
        return alert_log
