#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
from concurrent import futures

import six
import taskflow.engines
from oslo_log import log as logging
from taskflow import task
from taskflow.patterns import linear_flow as lf
from taskflow.types.failure import Failure

from DSpace import context
from DSpace import objects
from DSpace import version
from DSpace.common.config import CONF
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import TaskStatus
from DSpace.utils.coordination import COORDINATOR

logger = logging.getLogger(__name__)


class TaskFlowManager(object):
    _tasks = {}
    _names = {}

    def add(self, tf):
        logger.info("taskflow add %s", tf)
        self._tasks[tf.id] = tf
        name = tf.name
        if name not in self._names:
            self._names[name] = {}
        self._names[name][tf.id] = tf

    def pop(self, tf):
        logger.info("taskflow pop %s", tf)
        if tf.id not in self._tasks:
            logger.warning("taskflow %s(%s) not found in id list",
                           tf.name, tf.id)
        else:
            self._tasks.pop(tf.id, None)
        name = tf.name
        if name not in self._names:
            logger.warning("taskflow %s not found in name list", name)
        elif tf.id not in self._names[name]:
            logger.warning("taskflow %s(%s) not found in name list",
                           tf.name, tf.id)
        else:
            self._names[name].pop(tf.id, None)

    def is_name_exists(self, taskflow_name):
        if taskflow_name not in self._names:
            return False
        elif len(self._names[taskflow_name]) == 0:
            return False
        else:
            return True


task_manager = TaskFlowManager()


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
    def _get_task_name(self):
        name = self.name
        if '.' in name:
            name = name.split(".")[-1]
        return name

    def prepare_task(self, ctxt, tf):
        name = self._get_task_name()
        t = objects.Task(ctxt, name=name, status=TaskStatus.RUNNING,
                         taskflow_id=tf.id)
        t.create()
        self._task = t

    def finish_task(self):
        t = self._task
        t.finish()

    def failed_task(self):
        t = self._task
        t.failed()

    def revert(self, result, flow_failures):
        if isinstance(result, Failure):
            self.failed_task()


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
        self.coordinator = COORDINATOR
        self.lock = None

    def _create_tf(self):
        tf = objects.Taskflow(self.ctxt, name=self.name,
                              status=TaskStatus.RUNNING)
        tf.create()
        return tf

    def taskflow(self, **kwargs):
        wf = lf.Flow('TaskFlow')
        wf.add(ExampleTask())
        return wf

    def check_exists(self):
        """Check Taskflow exists

        If your task does not allow parallel execution,
        you need to call require_lock.
        """
        return task_manager.is_name_exists()

    def require_lock(self, lock_name=None, blocking=True):
        """Require lock

        If your task does not allow parallel execution,
        you need to call this first.
        """
        lock_name = lock_name or self.name
        self.lock = self.coordinator.get_lock(lock_name)
        return self.lock.acquire(blocking=blocking)

    def release_lock(self):
        if self.lock:
            self.lock.release()
            self.lock = None

    def run(self, **kwargs):
        tf = self._create_tf()
        wf = self.taskflow(**kwargs)
        store = kwargs
        store.update({
            "ctxt": self.ctxt,
            'tf': tf
        })
        try:
            task_manager.add(tf)
            taskflow.engines.run(wf, store=store)
            tf.finish()
        except Exception as e:
            msg = str(e)
            tf.failed(msg)
            raise e
        finally:
            task_manager.pop(tf)
            self.release_lock()


##################
# for test


class SleepTask(Task):
    def execute(self, ctxt, tf):
        self.prepare_task(ctxt, tf)
        logger.info("Before sleep")
        time.sleep(10)
        logger.info("After sleep")
        self.finish_task()


class TestTaskflow(Taskflow):
    def taskflow(self, **kwargs):
        logger.info("taskflow build")
        wf = lf.Flow('TestTaskflow')
        wf.add(SleepTask())
        return wf


def run_task(ctxt):
    logger.info("task new")
    t1 = TestTaskflow(ctxt)
    lock = t1.require_lock()
    logger.info("lock: %s", lock)
    t1.run()


def main():
    CONF(sys.argv[1:], project='stor',
         version=version.version_string())
    logging.setup(CONF, "stor")
    objects.register_all()
    COORDINATOR.start()
    ctxt = context.get_context()
    executor = futures.ThreadPoolExecutor(
        max_workers=CONF.task_workers)
    executor.submit(run_task, ctxt)
    executor.submit(run_task, ctxt)
    executor.shutdown(wait=True)
    # time.sleep(19)
    logger.info("shutdown")


if __name__ == '__main__':
    main()
