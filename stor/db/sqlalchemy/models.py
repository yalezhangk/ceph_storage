from oslo_config import cfg
from oslo_db.sqlalchemy import models
from oslo_utils import timeutils
from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

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


class Volume(BASE, StorBase):
    __tablename__ = "volumes"

    id = Column(String(36), primary_key=True)
    size = Column(Integer)
    status = Column(String(255))  # TODO(vish): enum?

    display_name = Column(String(255))
    display_description = Column(String(255))
