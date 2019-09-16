from concurrent import futures
import time
import logging
import threading
import socket

import etcd3
import grpc


logger = logging.getLogger(__name__)
_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class ServiceBase:
    name = None
    hostname = None
    rpc_endpoint = None
    rpc_ip = None
    rpc_port = None
    service = None

    def __init__(self):
        if not self.hostname:
            self.hostname = socket.gethostname()

    def rpc_register_service(self, server):
        pass

    def start_rpc(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.rpc_register_service(server)
        server.add_insecure_port('{}:{}'.format(self.rpc_ip, self.rpc_port))
        server.start()
        self.server = server

    def stop_rpc(self):
        self.server.stop(0)

    def register_endpoint(self):
        etcd = etcd3.client(host='172.159.4.11', port=2379)
        lease = None

        while True:
            if not lease:
                lease = etcd.lease(ttl=3)
                etcd.put('/t2stor/service/{}/{}'.format(self.service_name, self.hostname),
                         self.rpc_endpoint, lease=lease)
            time.sleep(2)
            res = lease.refresh()[0]
            if res.TTL == 0:
                lease = None
            logger.debug("lease refresh")

    def start_heartbeat(self):
        thread = threading.Thread(target=self.register_endpoint, args=())
        thread.daemon = True
        thread.start()


    def start(self):
        self.start_rpc()
        self.start_heartbeat()

    def stop(self):
        self.stop_rpc()

    def run(self):
        self.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            self.stop()

