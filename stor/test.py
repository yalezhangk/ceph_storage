import sys
import uuid

import tornado.ioloop
import tornado.web
from oslo_log import log as logging

from stor.context import RequestContext
from stor import objects
from stor.objects import base
from stor import version
from stor.common.config import CONF
from stor.api.handlers import get_routers

logger = logging.getLogger(__name__)




def main():
    objects.register_all()
    ctxt = RequestContext(user_id="xxx", project_id="stor", is_admin=False)
    volume = objects.Volume(id=str(uuid.uuid4()), display_name="Volume A")
    se = base.StorObjectSerializer()
    import ipdb; ipdb.set_trace()
    obj = se.serialize_entity(ctxt, volume)
    print(obj)




if __name__ == "__main__":
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "cinder")
    main()
