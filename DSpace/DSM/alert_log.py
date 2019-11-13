from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import exception as exc
from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects.fields import AllResourceType as Resource
from DSpace.utils.mail import send_mail

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

    def _per_send_email(self, alert_rule, alert_value, mail_conf, to_emails):
        for to_email in to_emails:
            # todo sysconfig get email template
            subject_conf = {
                'smtp_subject': 't2stor_alert:{}'.format(
                    alert_rule.type)}
            content_conf = {'smtp_context': alert_value}
            mail_conf.update({'smtp_name': 't2stor',
                              'smtp_to_email': to_email})
            try:
                send_mail(subject_conf, content_conf,
                          mail_conf)
                logger.info('send email success,to_email=%s', to_email)
            except Exception as e:
                logger.error(
                    'send email error,to_email=%s,%s', to_email, str(e))

    def _send_alert_email(self, ctxt, to_datas):
        for to_data in to_datas:
            # 1.get smtp conf
            cluster_id = to_data['cluster_id']
            alert_rule = to_data['alert_rule']
            alert_value = to_data['alert_value']
            is_smtp = objects.SysConfigList.get_all(
                ctxt, filters={"cluster_id": cluster_id,
                               'key': 'smtp_enabled'})
            if not is_smtp:
                logger.info('not yet deploy SMTP conf,'
                            'will not send alert emails')
                return
            if is_smtp[0].value != '1':
                logger.info('SMTP conf has closed, will not send alert emails')
                return
            mail_conf = {}
            smtp_configs = objects.SysConfigList.get_all(
                ctxt, filters={"cluster_id": cluster_id})
            keys = ['smtp_user', 'smtp_password', 'smtp_host', 'smtp_port',
                    'smtp_enable_ssl', 'smtp_enable_tls']
            for smtp_conf in smtp_configs:
                if smtp_conf.key in keys:
                    mail_conf[smtp_conf.key] = smtp_conf.value
            # 2. send mail
            # alert_rule -> alert_group -> email_group: send_email
            al_groups = alert_rule.alert_groups
            for al_group in al_groups:
                email_groups = al_group.email_groups
                for email_group in email_groups:
                    to_emails = email_group.emails.strip().split(',')
                    # send per email
                    self._per_send_email(
                        alert_rule, alert_value, mail_conf, to_emails)
        return True

    def _receive_datas(self, ctxt, receive_datas):
        # 1. receive_alert_data
        to_datas = []
        if not isinstance(receive_datas, list):
            raise exc.InvalidInput(
                message="param 'alert_messages' must a list")
        for alert in receive_datas:
            resource_name = None
            resource_id = None
            labels = alert.get('labels')
            alert_value = alert['annotations']['description']
            alert_name = labels['alertname']
            cluster_id = labels['cluster_id']
            alert_rules = objects.AlertRuleList.get_all(
                ctxt, filters={'type': alert_name, 'cluster_id': cluster_id})
            if not alert_rules:
                continue
            alert_rule = alert_rules[0]
            if not alert_rule.enabled:
                continue
            resource_type = alert_rule.resource_type
            if resource_type == Resource.NODE:
                hostname = labels['hostname']
                node = objects.NodeList.get_all(
                    ctxt, filters={'hostname': hostname,
                                   'cluster_id': cluster_id})
                if node:
                    resource_id = node[0].id
                    resource_name = hostname
                    logger.info('receive_alert,resource_type=%s,name=%s',
                                Resource.NODE, resource_name)
            elif resource_type == Resource.OSD:
                osd_id = labels['osd_id']
                osd = objects.OsdList.get_all(
                    ctxt, filters={'osd_id': int(osd_id),
                                   'cluster_id': cluster_id})
                if osd:
                    resource_id = osd[0].id
                    resource_name = "osd.%s", osd_id
                    logger.info('receive_alert,resource_type=%s,name=%s',
                                Resource.OSD, resource_name)
            elif resource_type == Resource.POOL:
                pool_id = labels['pool_id']
                pool = objects.PoolList.get_all(
                    ctxt, filters={'pool_id': int(pool_id),
                                   'cluster_id': cluster_id})
                if pool:
                    resource_name = pool[0].display_name
                    resource_id = pool[0].id
                    logger.info('receive_alert,resource_type=%s,name=%s',
                                Resource.POOL, resource_name)
            elif resource_type == Resource.CLUSTER:
                cluster_id = labels['cluster_id']
                cluster = objects.Cluster.get_by_id(ctxt, cluster_id)
                if cluster:
                    resource_name = cluster.display_name
                    resource_id = cluster.id
                    logger.info('receive_alert,resource_type=%s,name=%s',
                                Resource.CLUSTER, resource_name)
            else:
                continue
            # create alert_log
            if resource_id and resource_name:
                per_data = {'alert_rule': alert_rule,
                            'alert_value': alert_value,
                            'cluster_id': cluster_id}
                alert_log_data = {
                    'resource_type': resource_type,
                    'resource_name': resource_name,
                    'resource_id': resource_id,
                    'level': alert_rule.level,
                    'alert_value': alert_value,
                    'alert_rule_id': alert_rule.id,
                }
                alert_log = objects.AlertLog(ctxt, **alert_log_data)
                alert_log.create()
                logger.info('create an alert_log success,resource_type=%s,'
                            'resource_name=%s,level=%s',
                            alert_log_data['resource_type'],
                            alert_log_data['resource_name'],
                            alert_log_data['level'])
                to_datas.append(per_data)
        return to_datas

    def alert_log_get(self, ctxt, alert_log_id):
        return objects.AlertLog.get_by_id(ctxt, alert_log_id)

    def alert_log_update(self, ctxt, alert_log_id, data):
        alert_log = self.alert_log_get(ctxt, alert_log_id)
        readed = data.get('readed')
        if readed is not True:
            raise exc.InvalidInput(message="param 'readed' must be True")
        alert_log.readed = readed
        alert_log.save()
        return alert_log

    def alert_log_delete(self, ctxt, alert_log_id):
        alert_log = self.alert_log_get(ctxt, alert_log_id)
        alert_log.destroy()
        return alert_log

    def send_alert_messages(self, ctxt, receive_datas):
        # 1. receive_datas
        to_datas = self._receive_datas(ctxt, receive_datas)
        # 2. send_email
        self.task_submit(self._send_alert_email, ctxt, to_datas)
        logger.info('send_email tasks has begin')
        return True

    def alert_log_all_readed(self, ctxt, alert_log_data):
        filters = None
        readed = alert_log_data.get('readed')
        if readed is not True:
            raise exc.InvalidInput(message="param 'readed' must be True")
        logger.info('begin alert_log set all_readed')
        result = objects.AlertLogList.update(
            ctxt, filters, {'readed': True})
        return result

    def alert_logs_set_deleted(self, ctxt, alert_log_data):
        before_time = alert_log_data.get('before_time')
        if not before_time:
            raise exc.InvalidInput(message="param 'before_time' is required")
        logger.info('begin alert_log set deleted')
        filters = {'created_at': before_time}
        now = timeutils.utcnow()
        updates = {'deleted': True, 'deleted_at': now}
        result = objects.AlertLogList.update(
            ctxt, filters, updates)
        return result
