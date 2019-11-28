import json

from oslo_log import log as logging
from oslo_utils import timeutils

from DSpace import objects
from DSpace.DSM.base import AdminBaseHandler
from DSpace.objects import fields as s_fields

logger = logging.getLogger(__name__)


class ServiceHandler(AdminBaseHandler):

    def _check_service_status(self, services):
        for service in services:
            time_now = timeutils.utcnow(with_timezone=True)
            if service.updated_at:
                update_time = service.updated_at
            else:
                update_time = service.created_at
            time_diff = time_now - update_time
            if time_diff.total_seconds() > 60:
                service.status = s_fields.ServiceStatus.INACTIVE
                service.save()

    def services_get_all(self, ctxt, marker=None, limit=None, sort_keys=None,
                         sort_dirs=None, filters=None, offset=None):
        services = objects.ServiceList.get_all(
            ctxt, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)
        self._check_service_status(services)
        return services

    def service_get_count(self, ctxt, filters=None):
        return objects.ServiceList.get_count(ctxt, filters=filters)

    def service_update(self, ctxt, services):
        services = json.loads(services)
        logger.debug('Update service status')
        for s in services:
            filters = {
                "name": s['name'],
                "node_id": s['node_id']
            }
            service = objects.ServiceList.get_all(ctxt, filters=filters)
            if not service:
                service_new = objects.Service(
                    ctxt, name=s.get('name'), status=s.get('status'),
                    node_id=s.get('node_id'), cluster_id=ctxt.cluster_id,
                    counter=0
                )
                service_new.create()
            else:
                service = service[0]
                status = s.get('status')
                service.status = status
                if status == s_fields.ServiceStatus.ACTIVE:
                    service.counter += 1
                service.save()
        return True
