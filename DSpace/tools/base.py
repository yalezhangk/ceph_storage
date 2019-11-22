#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import subprocess

import paramiko
import six

from DSpace import exception
from DSpace.common.config import CONF
from DSpace.exception import RunCommandArgsError

logger = logging.getLogger(__name__)


def _bytes2str(string):
    return string.decode('utf-8') if isinstance(string, bytes) else string


class Executor(object):
    ssh = None
    host_prefix = None

    def __init__(self):
        """Command executor"""
        self.host_prefix = CONF.host_prefix

    def run_command(self, args, timeout=None):
        logger.debug("Run Command: {}".format(args))
        if not isinstance(args, (list, six.binary_type, six.text_type)):
            raise RunCommandArgsError()

        cmd = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = cmd.communicate()
        rc = cmd.returncode
        return (rc, _bytes2str(stdout), _bytes2str(stderr))

    def write(self, filename, content):
        f = open(filename, "w")
        f.write(content)
        f.close()


class SSHExecutor(Executor):
    ssh = None
    host_prefix = None

    def __init__(self, hostname=None, port=22, user='root', password=None,
                 pkey=None):
        """Command executor"""
        super(SSHExecutor, self).__init__()
        self.connect(hostname=hostname, port=port, user=user,
                     password=password, pkey=pkey)

    def connect(self, hostname=None, port=22, user='root', password=None,
                pkey=None):
        """connect remote host

        :param hostname: the host to connect
        :param port: ssh port
        :param user: ssh user
        :param password: ssh password
        :param pkey: the file-like object to read from
        """
        logger.info("try ssh connect: ip(%s), port(%s), user(%s), "
                    "password(%s), pkey(%s)",
                    hostname, port, user, password, pkey)
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = {}
        if password:
            kwargs['password'] = password
        if pkey:
            pkey = paramiko.RSAKey.from_private_key(pkey)
            kwargs['pkey'] = pkey

        try:
            self.ssh.connect(hostname, port=port, username=user, **kwargs)
        except paramiko.ssh_exception.AuthenticationException as e:
            logger.warning(e)
            raise exception.SSHAuthInvalid(ip=hostname, password=password)

    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None

    def __del__(self):
        self.close()

    def run_command(self, args, timeout=None):
        logger.debug("Run Command: {}".format(args))
        if not isinstance(args, (list, six.binary_type, six.text_type)):
            raise RunCommandArgsError()

        if isinstance(args, list):
            args = ' '.join(args)
        stdin, stdout, stderr = self.ssh.exec_command(args, timeout=timeout)
        rc = stdout.channel.recv_exit_status()
        # TODO: Need a better way.
        stdout, stderr = stdout.read(), stderr.read()
        return (rc, _bytes2str(stdout), _bytes2str(stderr))

    def write(self, filename, content):
        ftp = self.ssh.open_sftp()
        f = ftp.file(filename, "w", -1)
        f.write(content)
        f.flush()
        ftp.close()


class ToolBase(object):

    def __init__(self, executor):
        self.executor = executor

    def _wapper(self, path):
        host_prefix = self.executor.host_prefix
        if not host_prefix:
            return path
        if path[0] == os.path.sep:
            path = path[1:]
        return os.path.join(host_prefix, path)

    def run_command(self, args, **kwargs):
        return self.executor.run_command(args, **kwargs)


def test():
    """Test Executor"""
    ex = Executor()
    rc, out, err = ex.run_command(["ls", "/tmp"])
    print("Result:")
    print(rc)
    print(out)
    print(err)
    ex = SSHExecutor("127.0.0.1", 22, 'root')
    rc, out, err = ex.run_command(["ls", "/tmp"])
    print("Result:")
    print(rc)
    print(out)
    print(err)


if __name__ == '__main__':
    test()
