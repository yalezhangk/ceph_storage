from oslo_config import cfg
from oslo_db.sqlalchemy import models
from oslo_utils import timeutils
from sqlalchemy import BigInteger, Boolean, Column, DateTime
from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table
from sqlalchemy.orm import relationship


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
    table_id = Column(String(36), index=True)

    display_name = Column(String(255))
    display_description = Column(String(255))


class RPCService(BASE, StorBase):
    """Represents a block storage device that can be attached to a vm."""
    __tablename__ = 'rpc_services'

    id = Column(Integer, primary_key=True)
    service_name = Column(String(36))
    hostname = Column(String(36))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    endpoint = Column(String(255))


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


class Node(BASE, StorBase):
    __tablename__ = "nodes"

    id = Column(Integer, primary_key=True)
    hostname = Column(String(255))
    ip_address = Column(String(32))
    object_gateway_ip_address = Column(String(32))
    block_gateway_ip_address = Column(String(32))
    file_gateway_ip_address = Column(String(32))
    storage_cluster_ip_address = Column(String(32))
    storage_public_ip_address = Column(String(32))
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


class Disk(BASE, StorBase):
    __tablename__ = "disks"

    id = Column(Integer, primary_key=True)
    name = Column(String(32))
    status = Column(String(32))
    type = Column(String(32))
    disk_size = Column(BigInteger)  # bytes
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
    type = Column(String(32))
    role = Column(String(32), default='cache', index=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    disk_id = Column(Integer, ForeignKey('disks.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class Network(BASE, StorBase):
    __tablename__ = "networks"

    id = Column(Integer, primary_key=True)
    name = Column(String(32), index=True)
    status = Column(String(32), index=True)
    ip_address = Column(String(32))
    netmask = Column(String(32))
    mac_address = Column(String(32))
    type = Column(String(32))
    speed = Column(Integer)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class Service(BASE, StorBase):
    __tablename__ = "services"

    id = Column(Integer, primary_key=True)
    name = Column(String(32), index=True)
    node_id = Column(Integer, ForeignKey('nodes.id'))
    status = Column(String(32))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


osd_pools = Table(
    'osd_pools',
    BASE.metadata,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('osd_id', Integer, ForeignKey("osds.id")),
    Column('pool_id', Integer, ForeignKey("pools.id")),
    Column('cluster_id', String(36), ForeignKey('clusters.id'))
)


class Osd(BASE, StorBase):
    __tablename__ = "osds"

    id = Column(Integer, primary_key=True)
    name = Column(String(32), index=True)
    size = Column(BigInteger)  # bytes
    used = Column(BigInteger)  # bytes
    status = Column(String(32), index=True)
    type = Column(String(32), index=True)
    role = Column(String(32), index=True)
    fsid = Column(String(36))
    mem_read_cache = Column(BigInteger)  # bytes
    node_id = Column(Integer, ForeignKey('nodes.id'))
    disk_id = Column(Integer, ForeignKey('disks.id'))
    cache_partition_id = Column(Integer, ForeignKey('disk_partitions.id'))
    db_partition_id = Column(Integer, ForeignKey('disk_partitions.id'))
    wal_partition_id = Column(Integer, ForeignKey('disk_partitions.id'))
    journal_partition_id = Column(Integer, ForeignKey('disk_partitions.id'))
    pool_id = Column(Integer, ForeignKey('pools.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class Pool(BASE, StorBase):
    __tablename__ = "pools"

    id = Column(Integer, primary_key=True)
    display_name = Column(String(32), index=True)
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
    failure_domain_type = Column(String(32), default='host', index=True)
    crush_rule = Column(String(64))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    osds = relationship('osds', secondary=osd_pools, backref='nodes')


class Volume(BASE, StorBase):
    __tablename__ = "volumes"

    id = Column(Integer, primary_key=True)
    size = Column(BigInteger)
    used = Column(BigInteger)
    snapshot_num = Column(Integer)
    status = Column(String(255))  # TODO(vish): enum?
    display_name = Column(String(255))
    display_description = Column(String(255))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
    volume_access_path_id = Column(Integer,
                                   ForeignKey('volume_access_paths.id'))
    volume_client_group_id = Column(Integer,
                                    ForeignKey('volume_client_groups.id'))
    pool_id = Column(Integer, ForeignKey('pools.id'))
    snapshot_id = Column(Integer, ForeignKey('volume_snapshots.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class VolumeSnapshot(BASE, StorBase):
    __tablename__ = "volume_snapshots"

    id = Column(Integer, primary_key=True)
    uuid = Column(String(36))
    display_name = Column(String(255))
    is_protect = Column(Boolean, default=False)
    status = Column(String(32))
    size = Column(BigInteger)
    used = Column(BigInteger)
    volume_id = Column(Integer, ForeignKey('volumes.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class SysConfig(BASE, StorBase):
    __tablename__ = "sys_configs"

    id = Column(Integer, primary_key=True)
    service_id = Column(String(36))
    key = Column(String(255))
    value = Column(String(255))
    value_type = Column(String(36))
    display_description = Column(String(255))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class VolumeAccessPath(BASE, StorBase):
    __tablename__ = "volume_access_paths"

    id = Column(Integer, primary_key=True)
    iqn = Column(String(80))
    name = Column(String(32))
    status = Column(String(32))
    type = Column(String(32))
    chap_enable = Column(Boolean, default=False)  # 服务端对客户端的认证
    chap_username = Column(String(32))
    chap_password = Column(String(32))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


class VolumeAPGateway(BASE, StorBase):
    __tablename__ = "volume_ap_gateways"

    id = Column(Integer, primary_key=True)
    iqn = Column(String(80))
    node_id = Column(Integer, ForeignKey('nodes.id'))
    volume_access_path_id = Column(Integer,
                                   ForeignKey('volume_access_paths.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))


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
    volume_access_path_id = Column(Integer,
                                   ForeignKey('volume_access_paths.id'))
    cluster_id = Column(String(36), ForeignKey('clusters.id'))
