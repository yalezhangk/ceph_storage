#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys
import time
from concurrent import futures

import six
import taskflow.engines
from taskflow import task
from taskflow.patterns import linear_flow as lf
from taskflow.types.failure import Failure

from DSpace import context
from DSpace import exception
from DSpace import objects
from DSpace import taskflows
from DSpace import version
from DSpace.common.config import CONF
from DSpace.i18n import _
from DSpace.objects import fields as s_fields
from DSpace.objects.fields import TaskStatus
from DSpace.utils.coordination import COORDINATOR

logger = logging.getLogger(__name__)


# TODO: Common Registry
class TaskflowRegistry(object):
    _registry = None
    _taskflows = None

    def __new__(cls, *args, **kwargs):
        if not cls._registry:
            cls._registry = super(TaskflowRegistry, cls).__new__(
                cls, *args, **kwargs)
        return cls._registry

    def __init__(self, *args, **kwargs):
        if self._taskflows is None:
            self._taskflows = {}

    @classmethod
    def register(cls, tf_cls):
        logger.info("register taskflow %s", tf_cls)
        registry = cls()
        registry._register_class(tf_cls)
        return tf_cls

    def _register_class(self, cls):
        name = cls.obj_name()
        if name not in self._taskflows:
            logger.info("taskflow %s registered", name)
            setattr(taskflows, name, cls)


class TaskFlowManager(object):
    _tasks = {}
    _names = {}

    def __init__(self):
        self._executor = futures.ThreadPoolExecutor(
            max_workers=CONF.task_workers)

    def _wapper(self, fun, *args, **kwargs):
        try:
            fun(*args, **kwargs)
        except Exception as e:
            logger.exception("Unexpected exception: %s", e)

    def task_submit(self, fun, *args, **kwargs):
        self._executor.submit(self._wapper, fun, *args, **kwargs)

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

    def _mark_failed_task(self, ctxt):
        taskflow_objs = objects.TaskflowList.get_all(
            ctxt, filters={"status": TaskStatus.RUNNING})
        for t in taskflow_objs:
            cls = getattr(taskflows, t.name, None)
            if not cls:
                raise exception.TaskflowNotFound(taskflow_id=t.name)
            logger.info("taskflow %s mark failed", t.id)
            taskflow = cls.from_obj(ctxt, t)
            self.task_submit(taskflow.failed)

    def bootstrap(self, ctxt):
        logger.info("TaskFlowManager bootstrap")
        self._mark_failed_task(ctxt)


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
    enable_redo = False
    enable_clean = False

    def __init__(self, ctxt, action_log_id=None,
                 tf=None):
        self.ctxt = ctxt
        self.name = self.obj_name()
        self.coordinator = COORDINATOR
        self.lock = None
        self.tf = tf
        self.action_log_id = action_log_id

    @classmethod
    def obj_name(cls):
        """Return the object's name"""
        return cls.__name__

    @classmethod
    def from_obj(cls, ctxt, tf):
        return cls(ctxt, tf.action_log_id, tf)

    def create_tf(self, **kwargs):
        args = self.format_args(**kwargs)
        tf = objects.Taskflow(
            self.ctxt, name=self.name, enable_clean=self.enable_clean,
            enable_redo=self.enable_redo, status=TaskStatus.RUNNING,
            args=args, action_log_id=self.action_log_id)
        tf.create()
        self.tf = tf
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
        return task_manager.is_name_exists(self.name)

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

    def format_args(self, **kwargs):
        return None

    def run(self, **kwargs):
        """Run taskflow"""
        if not self.tf:
            tf = self.create_tf(**kwargs)
        else:
            tf = self.tf
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

    def redo(self, **kwargs):
        """Redo a failed taskflow"""
        raise NotImplementedError

    def clean(self, **kwargs):
        """Clean a failed taskflow"""
        raise NotImplementedError

    def failed(self, **kwargs):
        """Mark a taskflow failed"""
        tf = self.tf
        tf.failed(_("DSpace manager service stoped."))


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
    try:
        t1.run()
    except Exception as e:
        logger.exception(e)


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
