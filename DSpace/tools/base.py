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

    def __init__(self, host_prefix=None):
        """Command executor"""
        self.host_prefix = host_prefix or CONF.host_prefix

    def run_command(self, args, timeout=None):
        logger.debug("Run Command: {}".format(args))
        if not isinstance(args, (list, six.binary_type, six.text_type)):
            raise RunCommandArgsError()

        cmd = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = cmd.communicate(timeout=timeout)
        rc = cmd.returncode
        return (rc, _bytes2str(stdout), _bytes2str(stderr))

    def write(self, filename, content, mode="w"):
        f = open(filename, mode)
        f.write(content)
        f.close()


class SSHExecutor(Executor):
    ssh = None
    host_prefix = None

    def __init__(self, hostname=None, port=None, user=None,
                 password=None, pkey=None, timeout=5):
        """Command executor"""
        super(SSHExecutor, self).__init__()
        self.ssh_port = port or CONF.ssh_port
        # 1. If user is not specified, use CONF.ssh_user, default is root
        # 2. If password is not specified, use CONF.ssh_password, default is
        #    None
        self.user = user or CONF.ssh_user
        self.password = password or CONF.ssh_password
        self.connect(hostname=hostname, port=self.ssh_port, user=self.user,
                     password=self.password, pkey=pkey, timeout=timeout)
        self.host_prefix = None

    def connect(self, hostname=None, port=CONF.ssh_port, user='root',
                password=None, pkey=None, timeout=None):
        """connect remote host

        :param hostname: the host to connect
        :param port: ssh port
        :param user: ssh user
        :param password: ssh password
        :param pkey: the file-like object to read from
        :param timeout: connect timeout
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
            self.ssh.connect(hostname, port=port, username=user,
                             timeout=timeout, **kwargs)
        except paramiko.ssh_exception.AuthenticationException as e:
            logger.warning(e)
            raise exception.SSHAuthInvalid(ip=hostname, password=password)
        except paramiko.ssh_exception.PasswordRequiredException as e:
            logger.warning(e)
            # 需要提供密码
            raise exception.SSHPasswordRequiredException(ip=hostname)
        except paramiko.ssh_exception.BadAuthenticationType as e:
            logger.warning(e)
            # 不支持的认证类型
            raise exception.SSHBadAuthenticationType(ip=hostname)
        except paramiko.ssh_exception.BadAuthenticationType as e:
            logger.warning(e)
            raise exception.SSHBadAuthenticationType(ip=hostname)
        except paramiko.ssh_exception.PartialAuthentication as e:
            logger.warning(e)
            # 内部认证异常
            raise exception.SSHPartialAuthentication(ip=hostname)
        except paramiko.ssh_exception.ChannelException as e:
            logger.warning(e)
            # 打开新通道异常
            raise exception.SSHChannelException(ip=hostname)
        except paramiko.ssh_exception.BadHostKeyException as e:
            logger.warning(e)
            # 主机密钥不匹配
            raise exception.SSHBadHostKeyException(ip=hostname)
        except paramiko.ssh_exception.ProxyCommandFailure as e:
            logger.warning(e)
            # 请检查ssh配置文件
            raise exception.SSHProxyCommandFailure(ip=hostname)
        except Exception as e:
            logger.warning(e)
            # 无法连接
            raise exception.SSHConnectException(ip=hostname)

    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None

    def __del__(self):
        self.close()

    def _run_cmd(self, cmd_args, timeout=None):
        stdin, stdout, stderr = self.ssh.exec_command(
            cmd_args, timeout=timeout)
        # TODO timeout is not working, blocked here
        rc = stdout.channel.recv_exit_status()
        # TODO: Need a better way.
        stdout, stderr = stdout.read(), stderr.read()
        return rc, _bytes2str(stdout), _bytes2str(stderr)

    def _run_root_command(self, cmd_args, timeout=None):
        logger.debug("Run cmd as root user, run cmd: %s", cmd_args)
        return self._run_cmd(cmd_args, timeout)

    def _run_non_root_command(self, cmd_args, timeout=None,
                              root_permission=True):
        # Add sudo at the start of args if root permission required.
        if root_permission:
            if not self.password:
                cmd_args = CONF.sudo_prefix + " " + cmd_args
            else:
                cmd_args = "echo {} | sudo -S -p ' ' ".format(
                    self.password) + cmd_args
        logger.debug("User is not root, run cmd: %s", cmd_args)
        return self._run_cmd(cmd_args, timeout)

    def run_command(self, cmd_args, timeout=None, root_permission=True):
        """Run ssh cmd via paramiko

        :param cmd_args: command
        :param timeout: timeout
        :param root_permission: Command needs root permission. Default is True
        :return:
        """
        logger.debug("Run Command: {}".format(cmd_args))
        if not isinstance(cmd_args, (list, six.binary_type, six.text_type)):
            raise RunCommandArgsError()

        if isinstance(cmd_args, list):
            cmd_args = ' '.join(cmd_args)
        if self.user == "root":
            return self._run_root_command(cmd_args, timeout)
        else:
            return self._run_non_root_command(
                cmd_args, timeout, root_permission)

    def write(self, full_path, content, chmod=None, chown=None):
        logger.debug("Write content to file %s via ssh.", full_path)
        # Get filename
        filename = full_path.split('/')
        name = filename[-1]
        # Write file to /tmp/
        tmp_path = "/tmp/" + name + ".tmp"
        ftp = self.ssh.open_sftp()
        f = ftp.file(tmp_path, "w", -1)
        f.write(content)
        f.flush()
        ftp.close()
        # Copy file to destination
        self.run_command(["cp", tmp_path, full_path])
        if chmod:
            self.run_command(["chmod", chmod, full_path])
        if chown:
            self.run_command(["chown", chown, full_path])
        # Remove tmp file
        self.run_command(["rm", "-rf", tmp_path])


class ToolBase(object):

    def __init__(self, executor):
        self.executor = executor

    def _wapper(self, path):
        host_prefix = self.executor.host_prefix
        if not host_prefix:
            return path
        if path == os.path.sep:
            return host_prefix
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
    ex = SSHExecutor("192.168.18.6", port=22, user="system")
    rc, out, err = ex.run_command("ls /root")
    print("Result:")
    print("rc: ", rc)
    print("out: ", out)
    print("err: ", err)
    ex.write("/etc/ceph/ceph.test", "ceph = test", "777", "system:system")


if __name__ == '__main__':
    test()
