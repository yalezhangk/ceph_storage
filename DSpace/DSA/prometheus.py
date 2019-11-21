import json
import logging
import os

from DSpace.DSA.base import AgentBaseHandler

logger = logging.getLogger(__name__)


class PrometheusHandler(AgentBaseHandler):
    def prometheus_target_add(self, ctxt, ip, port, hostname, path):
        logger.info("Add to prometheus target file: %s, %s, %s",
                    ip, port, hostname)
        target = {
            "targets": [ip + ":" + port],
            "labels": {
                "hostname": hostname
            }
        }
        if os.path.exists(path):
            file = open(path, 'r')
            r = file.read()
            file.close()
            if r:
                targets = json.loads(r)
            else:
                targets = []
        else:
            targets = []
        if targets:
            for t in targets:
                if t['targets'] == target['targets'] \
                        and t['labels'] == target['labels']:
                    return
        targets.append(target)
        file = open(path, 'w')
        file.write(json.dumps(targets))
        file.close()

    def prometheus_target_remove(self, ctxt, ip, port, hostname, path):
        logger.info("Remove from prometheus target file: %s, %s, %s",
                    ip, port, hostname)
        target = {
            "targets": [ip + ":" + port],
            "labels": {
                "hostname": hostname
            }
        }
        if os.path.exists(path):
            file = open(path, 'r')
            r = file.read()
            file.close()
            if r:
                targets = json.loads(r)
            else:
                return
        else:
            logger.error("Targets file do not exist!")
            return
        if not targets:
            logger.info("Targets file is empty!")
            return
        for t in targets:
            if t['targets'] == target['targets'] \
                    and t['labels'] == target['labels']:
                targets.remove(t)
        file = open(path, 'w')
        file.write(json.dumps(targets))
        file.close()
