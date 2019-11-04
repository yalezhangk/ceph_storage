from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields
from DSpace.utils.mail import send_mail

logger = logging.getLogger(__name__)


class MailHandler(AdminBaseHandler):
    def send_mail(subject, content, config):
        send_mail(subject, content, config)

    def smtp_init(self, ctxt):
        data = [
            ("smtp_enabled", '0', 'string'),
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
        for sysconf in sysconfs:
            if sysconf.key in keys:
                result[sysconf.key] = sysconf.value
        return result

    def update_smtp(self, ctxt, smtp_enabled,
                    smtp_user, smtp_password,
                    smtp_host, smtp_port,
                    smtp_enable_ssl,
                    smtp_enable_tls):
        # TODO check a object exists
        sysconf = None
        if smtp_enabled:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_enabled", value=smtp_enabled,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_user:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_user", value=smtp_user,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_password:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_password", value=smtp_password,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_host:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_host", value=smtp_host,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_port:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_port", value=smtp_port,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_enable_ssl:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_enable_ssl", value=smtp_enable_ssl,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
        if smtp_enable_tls:
            sysconf = objects.SysConfig(
                ctxt, key="smtp_enable_tls", value=smtp_enable_tls,
                value_type=s_fields.SysConfigType.STRING)
            sysconf.create()
