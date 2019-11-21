from DSpace import exception as exc
from DSpace.i18n import _


def validate_ip(ip_str):
    sep = ip_str.split('.')
    if len(sep) != 4:
        raise exc.InvalidInput(_(
            'IP address {} format is incorrect!').format(ip_str))
    for i, x in enumerate(sep):
        int_x = int(x)
        if int_x < 0 or int_x > 255:
            raise exc.InvalidInput(_(
                'IP address {} format is incorrect!').format(ip_str))
    return True
