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
from DSpace.objects.fields import TaskStatus

logger = logging.getLogger(__name__)


class PrepareTask(task.Task):
    """Deprecated"""
    def execute(self, ctxt, task_info):
        return True

    def revert(self, task_info, result, flow_failures):
        t = task_info.get('task')
        reason = [v.exception_str for k, v in six.iteritems(flow_failures)]
        for k, v in six.iteritems(flow_failures):
            logger.error(v.exception)
            logger.error(v.traceback_str)

        if t:
            t.status = s_fields.TaskStatus.FAILED
            t.reason = ','.join(reason)
            t.save()


class CompleteTask(task.Task):
    """Deprecated"""
    def execute(self, task_info):
        t = task_info.get('task')
        t.status = s_fields.TaskStatus.SUCCESS
        t.save()


class BaseTask(task.Task):
    """Deprecated"""

    def execute(self, task_info):
        logger.info("BaseTask is Deprecated")
        pass


def create_flow(ctxt):
    t = objects.Task(
        ctxt,
        name="Example",
        description="Example",
        status=s_fields.TaskStatus.RUNNING,
    )
    t.create()
    wf = lf.Flow('TaskFlow')
    wf.add(PrepareTask("TaskPrepare"))
    wf.add(CompleteTask('Complete'))
    taskflow.engines.run(wf, store={
        "ctxt": ctxt,
        'task_info': {
            "task": t
        }
    })
    logger.info("Create flow run success")


class Task(task.Task):

    def prepare_task(self, ctxt, tf):
        t = objects.Task(ctxt, name=self.name, status=TaskStatus.RUNNING,
                         taskflow_id=tf.id)
        t.create()
        self._task = t

    def finish_task(self):
        t = self._task
        t.finish()


class ExampleTask(Task):
    def execute(self, ctxt, tf):
        self.prepare_task(ctxt, tf)
        logger.info("I am example")
        self.finish_task()


class Taskflow(object):
    def __init__(self, ctxt, name=None):
        self.ctxt = ctxt
        if not name:
            name = self.__class__.__name__
        self.name = name

    def _create_tf(self):
        tf = objects.Taskflow(self.ctxt, name=self.name,
                              status=TaskStatus.RUNNING)
        tf.create()
        return tf

    def taskflow(self, **kwargs):
        wf = lf.Flow('TaskFlow')
        wf.add(ExampleTask())
        return wf

    def run(self, **kwargs):
        tf = self._create_tf()
        wf = self.taskflow(**kwargs)
        store = kwargs
        store.update({
            "ctxt": self.ctxt,
            'tf': tf
        })
        try:
            taskflow.engines.run(wf, store=store)
            tf.finish()
        except Exception as e:
            msg = str(e)
            tf.failed(msg)


def main():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    ctxt = context.get_context()
    tf = Taskflow(ctxt)
    tf.run()


if __name__ == '__main__':
    main()
