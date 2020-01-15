from oslo_log import log as logging
from oslo_utils import strutils

from DSpace import objects
from DSpace.DSM.alert_rule import EmailHelper
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import AllActionType
from DSpace.objects.fields import AllResourceType
from DSpace.utils.mail import send_mail

logger = logging.getLogger(__name__)


class MailHandler(AdminBaseHandler):
    def send_mail(self, ctxt, subject, content, config):
        try:
            send_mail(subject, content, config)
            to_email = config.get('smtp_to_email')
            logger.info('send test email success, to_mail:%s', to_email)
        except Exception as e:
            logger.exception('send test email error: %s', e)
            return False
        return True

    def smtp_init(self, ctxt):
        data = [
            ("smtp_enabled", 'True', 'string'),
            ("smtp_user", '0', 'string'),
            ("smtp_password", '0', 'string'),
            ("smtp_host", '0', 'string'),
            ("smtp_port", '0', 'string'),
            ("smtp_enable_ssl", 'True', 'string'),
            ("smtp_enable_tls", 'Flase', 'string'),
        ]
        for key, value, value_type in data:
            cfg = objects.SysConfig(key=key, value=value,
                                    value_type=value_type)
            cfg.save()

    def smtp_get(self, ctxt):
        result = {}
        sysconfs = objects.SysConfigList.get_all(
            ctxt, filters={"cluster_id": ctxt.cluster_id})
        keys = ['smtp_enabled', 'smtp_user', 'smtp_password', 'smtp_host',
                'smtp_port', 'smtp_enable_ssl', 'smtp_enable_tls']
        for key in keys:
            result[key] = None
        for sysconf in sysconfs:
            if sysconf.key in keys:
                result[sysconf.key] = sysconf.value
                result['smtp_enabled'] = strutils.bool_from_string(
                    result['smtp_enabled'])
                result['smtp_enable_ssl'] = strutils.bool_from_string(
                    result['smtp_enable_ssl'])
                result['smtp_enable_tls'] = strutils.bool_from_string(
                    result['smtp_enable_tls'])
        return result

    def update_smtp(self, ctxt, data):
        # TODO check a object exists
        begin_action = self.begin_action(
            ctxt, resource_type=AllResourceType.SMTP_SYSCONF,
            action=AllActionType.UPDATE)
        sysconf = None
        for k, v in data.items():
            sysconf = objects.SysConfigList.get_all(ctxt,
                                                    filters={"key": k})
            if sysconf:
                sysconf[0].value = v
                sysconf[0].save()
            else:
                sysconf = objects.SysConfig(
                    ctxt, key=k, value=v,
                    value_type=s_fields.ConfigType.STRING)
                sysconf.create()
        self.finish_action(begin_action, None, 'smtp_sysconf',
                           sysconf)
        # 开关开启/关闭，告警中心相应改变
        smtp_enabled = data.get('smtp_enabled')
        notify_group = self.alert_watcher.get_notify(ctxt.cluster_id)
        if notify_group:
            if smtp_enabled is True:
                mail_conf = self.smtp_get(ctxt)
                mail_conf.pop('smtp_enabled')
                notify_group.append(EmailHelper('email_helper', mail_conf))
            elif smtp_enabled is False:
                notify_group.remove('email_helper')
        return sysconf
