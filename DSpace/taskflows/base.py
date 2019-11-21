#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

import six
import taskflow.engines
from oslo_log import log as logging
from taskflow import task
from taskflow.patterns import linear_flow as lf

from DSpace import context
from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.objects import fields as s_fields

logger = logging.getLogger(__name__)


class PrepareTask(task.Task):
    def execute(self, ctxt, task_info):
        t = objects.Task(
            ctxt,
            name=task_info.get('name'),
            description=task_info.get('description'),
            current=self.name,
            step_num=task_info.get('step_num'),
            status=s_fields.TaskStatus.RUNNING,
            step=0
        )
        t.create()
        task_info['task'] = t
        return True

    def revert(self, task_info, result, flow_failures):
        t = task_info.get('task')
        reason = [v.exception_str for k, v in six.iteritems(flow_failures)]

        if t:
            t.status = s_fields.TaskStatus.FAILED
            t.reason = ','.join(reason)
            t.save()


class CompleteTask(task.Task):
    def execute(self, task_info):
        t = task_info.get('task')
        t.status = s_fields.TaskStatus.SUCCESS
        t.save()


class BaseTask(task.Task):

    def execute(self, task_info):
        t = task_info.get('task')
        if not t:
            return
        t.current = self.name
        t.step += 1
        t.save()


def create_flow(ctxt):
    wf = lf.Flow('TaskFlow')
    wf.add(PrepareTask("TaskPrepare"))
    wf.add(CompleteTask('Complete'))
    taskflow.engines.run(wf, store={
        "ctxt": ctxt,
        'task_info': {
            "name": "name",
            "step_num": "5"
        }
    })
    logger.info("Create flow run success")


def main():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = context.get_context()
    create_flow(ctxt)


if __name__ == '__main__':
    main()
