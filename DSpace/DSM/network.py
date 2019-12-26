import six
from oslo_log import log as logging

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields

logger = logging.getLogger(__name__)


class NetworkHandler(AdminBaseHandler):
    def network_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                        sort_dirs=None, filters=None, offset=None,
                        expected_attrs=None):
        networks = objects.NetworkList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset,
            expected_attrs=expected_attrs
        )
        return networks

    def network_get_count(self, ctxt, filters=None):
        return objects.NetworkList.get_count(ctxt, filters=filters)

    def network_reporter(self, ctxt, networks, node_id):
        all_net_objs = objects.NetworkList.get_all(
            ctxt, filters={'node_id': node_id})
        all_nets = {
            net.name: net for net in all_net_objs
        }
        for name, data in six.iteritems(networks):
            logger.info("Check network %s: %s", name, data)
            if data.get('active'):
                status = s_fields.NetworkStatus.UP
            else:
                status = s_fields.NetworkStatus.DOWN
            if data.get('type') == 'ether':
                _type = s_fields.NetworkType.COPPER
            else:
                _type = s_fields.NetworkType.FIBER
            ipv4 = data.get('ipv4', {})
            if name in all_nets:
                net = all_nets.pop(name)
                net.speed = str(data.get('speed')) + "Mb/s"
                net.status = status
                net.ip_address = ipv4.get('address')
                net.netmask = ipv4.get("netmask")
                net.mac_address = data.get('macaddress')
                net.type = _type
                net.save()
                logger.info("Update network %s: %s", name, data)
            else:
                net = objects.Network(
                    ctxt,
                    name=name,
                    speed=str(data.get('speed')) + "Mb/s",
                    status=status,
                    ip_address=ipv4.get('address'),
                    netmask=ipv4.get("netmask"),
                    mac_address=data.get('macaddress'),
                    node_id=node_id,
                    type=_type
                )
                net.create()
                logger.info("Create network %s: %s", name, data)
        for name, net in six.iteritems(all_nets):
            logger.warning("Remove network %s", name)
            net.destroy()
