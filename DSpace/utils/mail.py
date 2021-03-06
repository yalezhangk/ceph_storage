import logging
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

from DSpace.i18n import _

logger = logging.getLogger(__name__)


def send_mail(subject, content, config):
    smtp_user = config.get("smtp_user")
    smtp_password = config.get("smtp_password")
    smtp_host = config.get("smtp_host")
    smtp_port = config.get("smtp_port")
    smtp_enable_ssl = config.get("enable_ssl")
    smtp_enable_tls = config.get("enable_tls")
    smtp_to_email = config.get("smtp_to_email")
    smtp_name = config.get("smtp_name")
    try:
        msg = MIMEText(content, 'plain', 'utf-8')
        msg['From'] = formataddr(
            [smtp_name,
             smtp_user])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr(
            [smtp_to_email, smtp_to_email])  # 对应收件人邮箱昵称、收件人邮箱账号
        msg['subject'] = subject  # 邮件的主题，也可以说是标题
        if smtp_enable_ssl is True:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
            server.login(smtp_user,
                         smtp_password)  # 括号中对应的是发件人邮箱账号、邮箱密码
        elif smtp_enable_tls is True:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
            server.login(smtp_user,
                         smtp_password)  # 括号中对应的是发件人邮箱账号、邮箱密码
        else:
            server = smtplib.SMTP()
            server.connect(smtp_host, smtp_port)
            server.login(smtp_user,
                         smtp_password)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(smtp_user, [smtp_to_email],
                        msg.as_string())
        server.quit()  # 关闭连接
    except Exception as e:
        logger.exception("send mail error: %s", e)
        raise e


def mail_template(alert_msg=None):
    msg = _("The cluster receives an alert message:\n"
            "    {}\nPlease deal with it timely").format(alert_msg)
    return msg


alert_rule_translation = {
    "cluster_usage": _("cluster_usage"),
    "cpu_usage": _("cpu_usage"),
    "memory_usage": _("memory_usage"),
    "sys_disk_usage": _("sys_disk_usage"),
    "osd_usage": _("osd_usage"),
    "pool_usage": _("pool_usage"),
    "service_status": _("service_status"),
    "osd_status": _("osd_status"),
    "disk_usage": _("disk_usage"),
    "transmit_bandwidth_usage": _("transmit_bandwidth_usage"),
    "receive_bandwidth_usage": _("receive_bandwidth_usage"),
    "object_user_size_usage": _("object_user_size_usage"),
    "object_bucket_size_usage": _("object_bucket_size_usage"),
    "disk_online": _("disk_online"),
    "disk_offline": _("disk_offline"),
}
