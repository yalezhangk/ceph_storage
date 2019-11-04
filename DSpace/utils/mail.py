import logging
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

logger = logging.getLogger(__name__)


def send_mail(config, subject, content):
    smtp_user = config.get("smtp_user")
    smtp_password = config.get("smtp_password")
    smtp_host = config.get("smtp_host")
    smtp_port = config.get("smtp_port")
    smtp_enable_ssl = config.get("smtp_enable_ssl")
    smtp_enable_tls = config.get("smtp_enable_tsl")
    smtp_name = config.get("smtp_name")
    smtp_subject = subject.get("smtp_subject")
    smtp_context = content.get("smtp_context")
    try:
        msg = MIMEText(smtp_context, 'plain', 'utf-8')
        msg['From'] = formataddr(
            [smtp_name,
             smtp_user])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr(
            [smtp_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg[smtp_subject] = smtp_subject  # 邮件的主题，也可以说是标题
        if smtp_enable_ssl is True:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
            server.login(smtp_user,
                         smtp_password)  # 括号中对应的是发件人邮箱账号、邮箱密码
        elif smtp_enable_tls is False:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
            server.login(smtp_user,
                         smtp_password)  # 括号中对应的是发件人邮箱账号、邮箱密码
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.login(smtp_user,
                         smtp_password)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(smtp_user, [smtp_user, ],
                        msg.as_string())
        server.quit()  # 关闭连接
    except Exception as e:
        logger.error("send mail error: %s", e)
