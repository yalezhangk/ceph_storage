# Last Update:2019-09-27 15:26:24
import re


def _cleanse_wwn(wwn_type, wwn):
    '''
    Some wwns may have alternate text representations. Adjust to our
    preferred representation.
    '''
    wwn = str(wwn.strip()).lower()

    if wwn_type in ('naa', 'eui', 'ib'):
        if wwn.startswith("0x"):
            wwn = wwn[2:]
        wwn = wwn.replace("-", "")
        wwn = wwn.replace(":", "")

        if not (wwn.startswith("naa.") or wwn.startswith("eui.") or
                wwn.startswith("ib.")):
            wwn = wwn_type + "." + wwn

    return wwn


def normalize_wwn(wwn):
    wwn_types = ('iqn', 'naa', 'eui')
    '''
    Take a WWN as given by the user and convert it to a standard text
    representation.

    Returns (normalized_wwn, wwn_type), or exception if invalid wwn.
    '''
    wwn_test = {
        'free': lambda wwn: True,
        'iqn': lambda wwn:
        re.match(r"iqn\.[0-9]{4}-[0-1][0-9]\..*\..*", wwn) and
        not re.search(' ', wwn) and
        not re.search('_', wwn),
        'naa': lambda wwn: re.match(r"naa\.[125][0-9a-fA-F]{15}$", wwn),
        'eui': lambda wwn: re.match(r"eui\.[0-9a-f]{16}$", wwn),
        'ib': lambda wwn: re.match(r"ib\.[0-9a-f]{32}$", wwn),
        'unit_serial': lambda wwn:
        re.match("[0-9A-Fa-f]{8}(-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}$", wwn),
    }

    for wwn_type in wwn_types:
        clean_wwn = _cleanse_wwn(wwn_type, wwn)
        found_type = wwn_test[wwn_type](clean_wwn)
        if found_type:
            break
    else:
        return (None, None)

    return (clean_wwn, wwn_type)
