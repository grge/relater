import requests
import json
import logging

from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.ext.associationproxy import association_proxy, _AssociationCollection
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.interfaces import MANYTOONE, MANYTOMANY, ONETOMANY
from sqlalchemy import Table, Column, ForeignKey, String, DateTime, func, Integer, Boolean
from sqlalchemy import create_engine

class Relater:
    last_updated = Column(DateTime, default=func.now())
    api_enpoint = None
    api_headers = None

    @declared_attr
    def __tablename__(cls):
        return cls.__name__

    def load_from_api(self, session):
        r = requests.get(self.api_endpoint, headers=self.api_headers)
        if r.status_code == 200 and 'application/json' in r.headers['Content-Type']:
                self.create_from_data(session, self.payload_accessor(json.loads(r.text)))
        else:
            logging.error("Received unexpected status code from %s endpoint: %s" % (self._class.__name__, r.status_code))

    @classmethod
    def create_from_data(cls, session, data):
        col_attrs = {}
        mapper = cls.__mapper__

        # Apply any transformations to the data first
        data = cls.data_transformer(data)

        # parse columns data
        for attr_name in data:
            mapper_name = attr_name.replace('-', '_')
            if mapper_name in mapper.columns:
                col_attrs[mapper_name] = data[attr_name]

        # create the class instance
        c = session.merge(cls(**col_attrs))

        for attr_name in data:
            mapper_name = attr_name.replace('-', '_')
            if mapper_name in mapper.relationships.keys():
                r = mapper.relationships[mapper_name]
                if r.direction == MANYTOONE:
                    child_object = session.merge(r.mapper.class_.create_from_data(session, data[attr_name]))
                    setattr(c, mapper_name, child_object)
                    c = session.merge(c)

                elif r.direction in (MANYTOMANY, ONETOMANY):
                    child_objects = [session.merge(r.mapper.class_.create_from_data(session, d)) for d in data[attr_name]
                            if not isinstance(d, str)]
                    setattr(c, mapper_name, child_objects)
                    c = session.merge(c)

                else:
                    print("Reltionship direction %s not yet implemented" % r.direction)
            elif mapper_name in mapper.columns:
                pass
            else:
                logging.warn("Attribute %s not found in data model of %s. Contents: %s" % (attr_name, cls.__tablename__, data[attr_name]))
        return(c)

    @classmethod
    def data_transformer(cls, data):
        return data

