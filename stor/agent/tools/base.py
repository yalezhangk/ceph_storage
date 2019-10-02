#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import subprocess
import paramiko
import six

from stor.exception import RunCommandArgsError


logger = logging.getLogger(__name__)


class Executor(object):
    ssh = None

    def __init__(self):
        """Command executor"""
        pass

    def connect(self, hostname=None, port=22, user='root', password=None,
                pkey=None):
        """connect remote host

        :param hostname: the host to connect
        :param port: ssh port
        :param user: ssh user
        :param password: ssh password
        :param pkey: the file-like object to read from
        """
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = {}
        if password:
            kwargs['password'] = password
        if pkey:
            pkey = paramiko.RSAKey.from_private_key(pkey)
            kwargs['pkey'] = pkey

        self.ssh.connect(hostname, port=port, username=user, **kwargs)

    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None

    def __del__(self):
        self.close()

    def run_command(self, args):
        logger.debug("Run Command: {}".format(args))
        if not isinstance(args, (list, six.binary_type, six.text_type)):
            raise RunCommandArgsError()

        if self.ssh:
            return self.run_remote_command(args)
        else:
            return self.run_local_command(args)

    def run_local_command(self, args):
        cmd = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        stdout, stderr = cmd.communicate()
        rc = cmd.returncode
        return (rc, stdout, stderr)

    def run_remote_command(self, args):
        cmd = ' '.join(args)
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        rc = stdout.channel.recv_exit_status()
        # TODO: Need a better way.
        stdout, stderr = stdout.read(), stderr.read()
        return (rc, stdout, stderr)


class ToolBase(object):
    def __init__(self, executor):
        self.executor = executor

    def run_command(self, args):
        return self.executor.run_command(args)


def test():
    """Test Executor"""
    ex = Executor()
    rc, out, err = ex.run_command(["ls", "/tmp"])
    print("Result:")
    print(rc)
    print(out)
    print(err)
    ex.connect("127.0.0.1", 22, 'root')
    rc, out, err = ex.run_command(["ls", "/tmp"])
    print("Result:")
    print(rc)
    print(out)
    print(err)


if __name__ == '__main__':
    test()
