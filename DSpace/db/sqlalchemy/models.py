from oslo_config import cfg
from oslo_db.sqlalchemy import models
from oslo_db.sqlalchemy.types import JsonEncodedDict
from oslo_utils import timeutils
from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship

from DSpace.db.sqlalchemy import types

CONF = cfg.CONF
BASE = declarative_base()


class StorBase(models.TimestampMixin,
               models.ModelBase):
    """Base class for Stor Models."""

    __table_args__ = {'mysql_engine': 'InnoDB'}

    # TODO(rpodolyaka): reuse models.SoftDeleteMixin in the next stage
    #                   of implementing of BP db-cleanup
    deleted_at = Column(DateTime)
    deleted = Column(Boolean, default=False)
    metadata = None

    @staticmethod
    def delete_values():
        return {'deleted': True,
                'deleted_at': timeutils.utcnow()}

    def delete(self, session):
        """Delete this object."""
        updated_values = self.delete_values()
        self.update(updated_values)
        self.save(session=session)
        return updated_values


class Cluster(BASE, StorBase):
    """Represents a block storage device that can be attached to a vm."""
    __tablename__ = 'clusters'

    id = Column(String(36), primary_key=True)

    display_name = Column(String(255))
    display_description = Column(String(255))
    is_admin = Column(Boolean, default=False)
    status = Column(String(64))
    ceph_status = Column(Boolean, default=False)


class RPCService(BASE, StorBase):
    """Represents a block storage device that can be attached to a vm."""
    __tablename__ = 'rpc_services'

    id = Column(Integer, primary_key=True)
    service_name = Column(String(36))
    hostname = Column(String(36))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    endpoint = Column(JsonEncodedDict())
    node_id = Column(Integer, ForeignKey('nodes.id'))


class Datacenter(BASE, StorBase):
    __tablename__ = "datacenters"

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    cluster_id = Column(String(36))


class Rack(BASE, StorBase):
    __tablename__ = "racks"

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    datacenter_id = Column(Integer, ForeignKey('datacenters.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    _nodes = relationship("Node", backref="_rack")


class Node(BASE, StorBase):
    __tablename__ = "nodes"

    id = Column(Integer, primary_key=True)
    hostname = Column(String(255))
    ip_address = Column(String(32))
    object_gateway_ip_address = Column(types.IPAddress())
    block_gateway_ip_address = Column(types.IPAddress())
    file_gateway_ip_address = Column(types.IPAddress())
    cluster_ip = Column(types.IPAddress())
    public_ip = Column(types.IPAddress())
    password = Column(String(32))
    status = Column(String(255))
    role_admin = Column(Boolean, default=False)
    role_monitor = Column(Boolean, default=False)
    role_storage = Column(Boolean, default=False)
    role_block_gateway = Column(Boolean, default=False)
    role_object_gateway = Column(Boolean, default=False)
    role_file_gateway = Column(Boolean, default=False)
    vendor = Column(String(255))
    model = Column(String(255))
    cpu_num = Column(Integer)
    cpu_model = Column(String(255))
    cpu_core_num = Column(Integer)
    mem_size = Column(BigInteger)
    sys_type = Column(String(255))
    sys_version = Column(String(255))
    rack_id = Column(String(36), ForeignKey('racks.id'))
    time_diff = Column(BigInteger)
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    _disks = relationship("Disk", backref="_node")
    _networks = relationship("Network", backref="_node")
    _osds = relationship("Osd", backref="_node")
    _radosgws = relationship("Radosgw", backref="_node")


class Disk(BASE, StorBase):
    __tablename__ = "disks"

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    status = Column(String(32))
    type = Column(String(32))
    size = Column(BigInteger)  # bytes
    rotate_speed = Column(Integer)  # 转速
    slot = Column(String(32))  # 插槽
    model = Column(String(32))
    vendor = Column(String(32))
    support_led = Column(Boolean, default=False)
    led = Column(String(3), default='off')
    has_patrol = Column(Boolean, default=False)
    patrol_data = Column(String(2048))
    residual_life = Column(Integer)  # 剩余寿命
    role = Column(String(32), default='data', index=True)
    partition_num = Column(Integer)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class DiskPartition(BASE, StorBase):
    __tablename__ = "disk_partitions"

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    size = Column(BigInteger)  # bytes
    status = Column(String(32))
    uuid = Column(String(36))  # partition uuid
    type = Column(String(32))
    role = Column(String(32), index=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    disk_id = Column(Integer, ForeignKey('disks.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    _disk = relationship("Disk", backref="_partitions")
    _node = relationship("Node", backref="_partitions")


class Network(BASE, StorBase):
    __tablename__ = "networks"

    id = Column(Integer, primary_key=True)
    name = Column(String(32), index=True)
    status = Column(String(32), index=True)
    ip_address = Column(String(32))
    netmask = Column(String(32))
    mac_address = Column(String(32))
    type = Column(String(32))
    speed = Column(String(32))
    node_id = Column(Integer, ForeignKey('nodes.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class Service(BASE, StorBase):
    __tablename__ = "services"

    id = Column(Integer, primary_key=True)
    name = Column(String(32), index=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    status = Column(String(32))
    role = Column(String(32))
    counter = Column(BigInteger, default=0)
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class Osd(BASE, StorBase):
    __tablename__ = "osds"

    id = Column(Integer, primary_key=True)
    osd_id = Column(String(32), index=True)
    size = Column(BigInteger)  # bytes
    used = Column(BigInteger)  # bytes
    status = Column(String(32), index=True)
    type = Column(String(32), index=True)
    disk_type = Column(String(32), index=True)
    fsid = Column(String(36))
    mem_read_cache = Column(BigInteger)  # bytes
    node_id = Column(Integer, ForeignKey('nodes.id'))
    disk_id = Column(Integer, ForeignKey('disks.id'))
    cache_partition_id = Column(Integer, ForeignKey('disk_partitions.id'))
    db_partition_id = Column(Integer, ForeignKey('disk_partitions.id'))
    wal_partition_id = Column(Integer, ForeignKey('disk_partitions.id'))
    journal_partition_id = Column(Integer, ForeignKey('disk_partitions.id'))
    crush_rule_id = Column(Integer, ForeignKey('crush_rules.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    _disk = relationship("Disk", backref=backref("_osd", uselist=False))
    _db_partition = relationship("DiskPartition",
                                 foreign_keys=[db_partition_id],
                                 backref=backref("_osd_db", uselist=False))
    _wal_partition = relationship("DiskPartition",
                                  foreign_keys=[wal_partition_id],
                                  backref=backref("_osd_wal", uselist=False))
    _cache_partition = relationship(
        "DiskPartition",
        foreign_keys=[cache_partition_id],
        backref=backref("_osd_cacahe", uselist=False))
    _journal_partition = relationship(
        "DiskPartition",
        foreign_keys=[journal_partition_id],
        backref=backref("_osd_journal", uselist=False))


class Pool(BASE, StorBase):
    __tablename__ = "pools"

    id = Column(Integer, primary_key=True)
    display_name = Column(String(64), index=True)
    pool_id = Column(Integer)
    pool_name = Column(String(64))
    type = Column(String(32))  # ec or replica pool
    data_chunk_num = Column(Integer)  # 数据块数量(ec pool)
    coding_chunk_num = Column(Integer)  # 校验块数量(ec pool)
    replicate_size = Column(Integer)  # 副本数
    role = Column(String(32), index=True)
    status = Column(String(32), index=True)
    size = Column(BigInteger)  # bytes
    used = Column(BigInteger)  # bytes
    osd_num = Column(Integer)
    speed_type = Column(String(32))
    crush_rule_id = Column(Integer, ForeignKey('crush_rules.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    rgw_zone_id = Column(Integer, ForeignKey('radosgw_zones.id'))


class Volume(BASE, StorBase):
    __tablename__ = "volumes"

    id = Column(Integer, primary_key=True)
    volume_name = Column(String(64))
    size = Column(BigInteger)
    used = Column(BigInteger)
    is_link_clone = Column(Boolean, default=False)  # 默认独立克隆
    snapshot_num = Column(Integer)
    status = Column(String(64))  # TODO(vish): enum?
    display_name = Column(String(255))
    display_description = Column(String(255))
    pool_id = Column(Integer, ForeignKey('pools.id'))
    snapshot_id = Column(Integer)
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    _snapshots = relationship("VolumeSnapshot", backref="_volume")


class VolumeSnapshot(BASE, StorBase):
    __tablename__ = "volume_snapshots"

    id = Column(Integer, primary_key=True)
    uuid = Column(String(36))
    display_name = Column(String(255))
    is_protect = Column(Boolean, default=True)
    status = Column(String(32))
    display_description = Column(String(255))
    volume_id = Column(Integer, ForeignKey('volumes.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


volume_access_path_gateways = Table(
    'volume_access_path_gateways', BASE.metadata,
    Column('created_at', DateTime),
    Column('updated_at', DateTime),
    Column('deleted_at', DateTime),
    Column('deleted', Boolean),
    Column('id', Integer, primary_key=True),
    Column('volume_access_path_id', Integer,
           ForeignKey("volume_access_paths.id")),
    Column('volume_gateway_id', Integer,
           ForeignKey("volume_gateways.id")),
    Column('cluster_id', String(36), ForeignKey('clusters.id'))
)


class VolumeMapping(BASE, StorBase):
    __tablename__ = "volume_mappings"

    id = Column(Integer, primary_key=True)
    volume_id = Column(Integer, ForeignKey("volumes.id"))
    volume_access_path_id = Column(
        Integer, ForeignKey("volume_access_paths.id"))
    volume_client_group_id = Column(
        Integer, ForeignKey("volume_client_groups.id"))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class VolumeAccessPath(BASE, StorBase):
    __tablename__ = "volume_access_paths"

    id = Column(Integer, primary_key=True)
    iqn = Column(String(80))
    name = Column(String(32))
    status = Column(String(32))
    type = Column(String(32))  # FC of ISCSI
    chap_enable = Column(Boolean, default=False)  # 服务端对客户端的认证
    chap_username = Column(String(32))
    chap_password = Column(String(32))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    _volume_gateways = relationship(
        'VolumeGateway',
        secondary=volume_access_path_gateways,
        back_populates='_volume_access_paths')


class VolumeGateway(BASE, StorBase):
    __tablename__ = "volume_gateways"

    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    _volume_access_paths = relationship(
        'VolumeAccessPath',
        secondary=volume_access_path_gateways,
        back_populates='_volume_gateways')


class VolumeClient(BASE, StorBase):
    __tablename__ = "volume_clients"

    id = Column(Integer, primary_key=True)
    client_type = Column(String(32))
    iqn = Column(String(80))
    volume_client_group_id = Column(Integer,
                                    ForeignKey('volume_client_groups.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class VolumeClientGroup(BASE, StorBase):
    __tablename__ = "volume_client_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    type = Column(String(32))
    chap_enable = Column(Boolean, default=False)  # 客户端对服务端的认证
    chap_username = Column(String(32))
    chap_password = Column(String(32))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class SysConfig(BASE, StorBase):
    __tablename__ = "sys_configs"

    id = Column(Integer, primary_key=True)
    key = Column(String(255))
    value = Column(Text())
    value_type = Column(String(36))
    display_description = Column(String(255))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class CephConfig(BASE, StorBase):
    __tablename__ = "ceph_configs"

    id = Column(Integer, primary_key=True)
    group = Column(String(255))
    key = Column(String(255))
    value = Column(String(255))
    value_type = Column(String(36))
    display_description = Column(String(255))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class LicenseFile(BASE, StorBase):
    """License_file db, save binary license file"""
    __tablename__ = 'license_files'

    id = Column(Integer, primary_key=True)
    content = Column(String(2048))
    status = Column(String(32))


class LogFile(BASE, StorBase):
    """Mon/Osd log file metadata info"""
    __tablename__ = 'log_files'

    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    service_type = Column(String(32))
    directory = Column(String(255))
    filename = Column(String(64))
    filesize = Column(Integer)
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


# 告警通知与告警规则多对多关系
alert_group_relate_rule = Table(
    'alert_group_relate_rule', BASE.metadata,
    Column('created_at', DateTime),
    Column('updated_at', DateTime),
    Column('deleted_at', DateTime),
    Column('deleted', Boolean),
    Column('id', Integer, primary_key=True),
    Column('alert_groups_id', Integer, ForeignKey("alert_groups.id")),
    Column('alert_rules_id', Integer, ForeignKey("alert_rules.id")),
    Column('cluster_id', String(36), ForeignKey('clusters.id'))
)


# 告警通知与邮件组多对多关系
alert_group_relate_email = Table(
    'alert_group_relate_email', BASE.metadata,
    Column('created_at', DateTime),
    Column('updated_at', DateTime),
    Column('deleted_at', DateTime),
    Column('deleted', Boolean),
    Column('id', Integer, primary_key=True),
    Column('alert_groups_id', Integer, ForeignKey("alert_groups.id")),
    Column('email_groups_id', Integer, ForeignKey("email_groups.id")),
    Column('cluster_id', String(36), ForeignKey('clusters.id'))
)


class AlertGroup(BASE, StorBase):
    """Alert group 告警通知组, 组合:哪些规则需要通知哪些邮件组"""
    __tablename__ = 'alert_groups'

    id = Column(Integer, primary_key=True)
    name = Column(String(64))
    alert_rules = relationship('AlertRule', secondary=alert_group_relate_rule,
                               back_populates='alert_groups')
    email_groups = relationship('EmailGroup',
                                secondary=alert_group_relate_email,
                                back_populates='alert_groups')
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class AlertRule(BASE, StorBase):
    """ Alert rule 告警规则"""
    __tablename__ = 'alert_rules'

    id = Column(Integer, primary_key=True)
    resource_type = Column(String(32))
    type = Column(String(64))  # 告警类型
    trigger_value = Column(String(64))  # 触发值 eg:>80%
    level = Column(String(64))  # 告警级别
    trigger_period = Column(String(64))  # 告警周期(分钟)
    enabled = Column(Boolean, default=False)  # 是否启用
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    alert_groups = relationship(
        'AlertGroup', secondary=alert_group_relate_rule,
        back_populates='alert_rules')


class EmailGroup(BASE, StorBase):
    """Email group 邮件组"""
    __tablename__ = 'email_groups'

    id = Column(Integer, primary_key=True)
    name = Column(String(64))
    emails = Column(String(1024))  # 收件地址组
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    alert_groups = relationship('AlertGroup',
                                secondary=alert_group_relate_email,
                                back_populates='email_groups')


class AlertLog(BASE, StorBase):
    """告警记录表"""
    __tablename__ = 'alert_logs'

    id = Column(Integer, primary_key=True)
    readed = Column(Boolean, default=False)
    resource_type = Column(String(32))
    level = Column(String(32))
    alert_value = Column(String(1024))
    resource_id = Column(String(64))
    resource_name = Column(String(64))
    alert_rule_id = Column(Integer, ForeignKey('alert_rules.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class ActionLog(BASE, StorBase):
    """操作记录表"""
    __tablename__ = 'action_logs'

    id = Column(Integer, primary_key=True)
    begin_time = Column(DateTime)
    finish_time = Column(DateTime)
    client_ip = Column(String(64))
    user_id = Column(String(32))
    action = Column(String(32))
    resource_id = Column(String(36))
    resource_name = Column(String(64))
    resource_type = Column(String(32))
    before_data = Column(Text())  # before_obj
    after_data = Column(Text())  # after_obj
    diff_data = Column(Text())  # diff description
    status = Column(String(32), default='under way')  # success/under way/fail
    err_msg = Column(Text())
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class CrushRule(BASE, StorBase):
    __tablename__ = 'crush_rules'

    id = Column(Integer, primary_key=True)
    rule_name = Column(String(32))
    rule_id = Column(Integer)
    type = Column(String(32))
    content = Column(JsonEncodedDict())
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    _osds = relationship("Osd", backref="_crush_rule")
    _pools = relationship("Pool", backref="_crush_rule")


class User(BASE, StorBase):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(64))
    password = Column(String(255))
    current_cluster_id = Column(String(36), nullable=True)


class Taskflow(BASE, StorBase):
    __tablename__ = 'taskflows'
    id = Column(Integer, primary_key=True)
    name = Column(String(64))
    description = Column(String(255))
    status = Column(String(32))
    reason = Column(Text)
    finished_at = Column(DateTime)
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class Task(BASE, StorBase):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    name = Column(String(64))
    status = Column(String(32))
    finished_at = Column(DateTime)
    taskflow_id = Column(Integer, ForeignKey('taskflows.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class Radosgw(BASE, StorBase):
    __tablename__ = 'radosgws'

    id = Column(Integer, primary_key=True)
    description = Column(String(255))
    name = Column(String(64))
    display_name = Column(String(255))
    status = Column(String(32))
    ip_address = Column(String(32))
    port = Column(Integer)
    zone_id = Column(Integer, ForeignKey('radosgw_zones.id'))
    node_id = Column(Integer, ForeignKey('nodes.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    counter = Column(BigInteger, default=0)
    router_id = Column(Integer, ForeignKey('radosgw_routers.id'))
    _radosgw_routers = relationship("RadosgwRouter", backref="_radosgws")


class RadosgwZone(BASE, StorBase):
    __tablename__ = 'radosgw_zones'

    id = Column(Integer, primary_key=True)
    description = Column(String(255))
    name = Column(String(64))
    zone_id = Column(String(36))
    zonegroup = Column(String(64))
    realm = Column(String(64))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class RadosgwRouter(BASE, StorBase):
    __tablename__ = 'radosgw_routers'

    id = Column(Integer, primary_key=True)
    description = Column(String(255))
    name = Column(String(64))
    status = Column(String(32))
    virtual_ip = Column(String(32))
    port = Column(Integer)
    https_port = Column(Integer)
    virtual_router_id = Column(Integer)
    nodes = Column(String(1024))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class RouterService(BASE, StorBase):
    __tablename__ = "router_services"

    id = Column(Integer, primary_key=True)
    name = Column(String(32), index=True)
    status = Column(String(32))
    node_id = Column(Integer, ForeignKey('nodes.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    net_id = Column(String(36), ForeignKey('networks.id'))
    router_id = Column(Integer, ForeignKey('radosgw_routers.id'))
    counter = Column(BigInteger, default=0)
    _radosgw_routers = relationship("RadosgwRouter",
                                    backref="_router_services")
