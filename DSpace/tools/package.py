#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import six

from DSpace.common.config import CONF
from DSpace.exception import RunCommandError
from DSpace.tools.base import ToolBase
from DSpace.tools.file import File as FileTool
from DSpace.utils import cluster_config
from DSpace.utils import template

logger = logging.getLogger(__name__)


class PackageBase(ToolBase):
    def install(self, names, **kwargs):
        raise NotImplementedError("Method Not ImplementedError")

    def uninstall(self, names, **kwargs):
        raise NotImplementedError("Method Not ImplementedError")

    def clean(self):
        raise NotImplementedError("Method Not ImplementedError")

    def render_repo_template(self, repo_template, **kwargs):
        logger.info("rendering template: %s, args: %s", repo_template, kwargs)
        tpl = template.get(repo_template)
        template_content = tpl.render(**kwargs)
        return template_content


class YumPackage(PackageBase):
    def install(self, names, **kwargs):
        logger.debug("Install Package: {}".format(names))
        cmd = ["yum", "install", "-y",
               "--setopt=skip_missing_names_on_install=False"]
        enable_repos = kwargs.pop("enable_repos", None)
        if enable_repos:
            if isinstance(enable_repos, six.string_types):
                enable_repos = [enable_repos]
            cmd.append("--disablerepo=*")
            cmd.append("--enablerepo={}".format(','.join(enable_repos)))
        if isinstance(names, six.string_types):
            names = [names]
        cmd.extend(names)
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def install_docker(self):
        docker_pkgs = ["docker-ce", "docker-ce-cli", "containerd.io"]
        self.install(docker_pkgs)

    def install_rgw(self):
        rgw_pkgs = ["ceph-radosgw"]
        self.install(rgw_pkgs)

    def uninstall(self, names):
        logger.debug("Uninstall Package: {}".format(names))
        cmd = ["yum", "remove", '-y']
        if isinstance(names, six.string_types):
            names = [names]
        cmd.extend(names)
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def uninstall_nodeps(self, packages):
        logger.debug("Uninstall Package: {}".format(packages))
        for package in packages:
            cmd = ["rpm", "-e", "--nodeps", package]
            rc, stdout, stderr = self.run_command(cmd)
            if rc == 0:
                logger.info("uninstall package: %s success", package)
                continue
            elif rc == 1:
                logger.info("uninstall package: %s notfound", package)
                continue
            else:
                raise RunCommandError(cmd=cmd, return_code=rc,
                                      stdout=stdout, stderr=stderr)

    def uninstall_rgw(self):
        rgw_pkgs = ["ceph-radosgw"]
        self.uninstall_nodeps(rgw_pkgs)

    def clean(self):
        logger.debug("Clean all Package cache")
        cmd = ["yum", "clean", 'all']
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def _update_cache(self):
        cmd = ["yum", "makecache"]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            logger.info("update all package cache")
            return True
        logger.error("update package cache error")
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def render_repo(self, repo_name, **kwargs):
        repo_template = "{}.repo.j2".format(repo_name)
        repo_content = self.render_repo_template(repo_template, **kwargs)
        logger.info("render_repo, repo_content: %s", repo_content)
        return repo_content

    def backup_repo(self, repo_name):
        repo_file = "/etc/yum.repos.d/{}.repo".format(repo_name)
        repo_backup_dir = "/etc/yum.repos.d/dspace-bak/"
        # backup repo
        file_tool = FileTool(self.executor)
        file_tool.mkdir(repo_backup_dir)
        if file_tool.exist(repo_file):
            file_tool.mv(repo_file, repo_backup_dir)

    def configure_repo(self, repo_name, repo_content, **kwargs):
        repo_file = "/etc/yum.repos.d/{}.repo".format(repo_name)
        file_tool = FileTool(self.executor)
        # set repo
        file_tool.write(repo_file, repo_content)
        self.clean()
        self._update_cache()


class AptPackage(PackageBase):
    def install(self, names, **kwargs):
        logger.debug("Install Package: {}".format(names))
        cmd = ["apt", "install", "-y"]
        if isinstance(names, six.string_types):
            names = [names]
        cmd.extend(names)
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        logger.error("install package %s error: %s", names, stderr)
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def install_docker(self):
        # curl for loading docker image
        docker_pkgs = ["docker-ce", "curl"]
        self.install(docker_pkgs)

    def install_rgw(self):
        rgw_pkgs = ["radosgw"]
        self.install(rgw_pkgs)

    def uninstall(self, names):
        logger.debug("Uninstall Package: {}".format(names))
        cmd = ["apt", "remove", "--purge", "-y"]
        if isinstance(names, six.string_types):
            names = [names]
        cmd.extend(names)
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            return True
        logger.error("uninstall package %s error: %s", names, stderr)
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def uninstall_nodeps(self, packages):
        self.uninstall(packages)

    def uninstall_rgw(self):
        rgw_pkgs = ["radosgw"]
        self.uninstall(rgw_pkgs)

    def clean(self):
        cmd = ["apt", "clean"]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            logger.info("clean all package cache")
            return True
        logger.error("clean package cache error")
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def _update_cache(self):
        cmd = ["apt", "update"]
        rc, stdout, stderr = self.run_command(cmd)
        if not rc:
            logger.info("update all package cache")
            return True
        logger.error("update package cache error")
        raise RunCommandError(cmd=cmd, return_code=rc,
                              stdout=stdout, stderr=stderr)

    def render_repo(self, repo_name, **kwargs):
        repo_template = "{}.list.j2".format(repo_name)
        repo_content = self.render_repo_template(repo_template, **kwargs)
        logger.info("render_repo, repo_content: %s", repo_content)
        return repo_content

    def backup_repo(self, repo_name):
        repo_file = "/etc/apt/sources.list"
        repo_backup_file = "/etc/apt/sources.list.back"
        file_tool = FileTool(self.executor)
        # backup repo
        if file_tool.exist(repo_file):
            file_tool.mv(repo_file, repo_backup_file)

    def configure_repo(self, repo_name, repo_content, **kwargs):
        repo_file = "/etc/apt/sources.list"
        file_tool = FileTool(self.executor)
        # set repo
        file_tool.write(repo_file, repo_content)
        self.clean()
        self._update_cache()


class Package(ToolBase):
    def __init__(self, executor, *args, **kwargs):
        super(Package, self).__init__(executor, *args, **kwargs)
        os_distro = CONF.os_distro
        logger.info("current os distro: %s", os_distro)
        self.pkg_mgr = cluster_config.PKG_MGR[os_distro]
        if self.pkg_mgr == "yum":
            self.tool = YumPackage(executor)
        elif self.pkg_mgr == "apt":
            self.tool = AptPackage(executor)
        else:
            logger.error("unknown os distro: %s", os_distro)
            raise NotImplementedError("unknown package manager, not implement")

    def install(self, names, **kwargs):
        self.tool.install(names=names, **kwargs)

    def uninstall(self, names, **kwargs):
        self.tool.uninstall(names=names, **kwargs)

    def uninstall_nodeps(self, packages, **kwargs):
        self.tool.uninstall_nodeps(packages=packages, **kwargs)

    def clean(self):
        self.tool.clean()

    def install_docker(self):
        self.tool.install_docker()

    def install_rgw(self):
        self.tool.install_rgw()

    def uninstall_rgw(self):
        self.tool.uninstall_rgw()

    def render_repo(self, repo_name, **kwargs):
        return self.tool.render_repo(repo_name=repo_name, **kwargs)

    def backup_repo(self, repo_name):
        self.tool.backup_repo(repo_name)

    def configure_repo(self, repo_name, repo_content, **kwargs):
        self.tool.configure_repo(repo_name=repo_name,
                                 repo_content=repo_content, **kwargs)
