import re
import musicbrainzngs as mb
from pprint import pprint
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.ext.associationproxy import association_proxy, _AssociationCollection
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm.interfaces import MANYTOONE, MANYTOMANY, ONETOMANY
from sqlalchemy import Table, Column, ForeignKey, String, DateTime, func, Integer, Boolean
from sqlalchemy import create_engine

mb.set_useragent("Musicbrainz SQLAlchemy Test", "0.1")
engine = create_engine('sqlite:///out.db')
Base = declarative_base()
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

def snake2camel(name):
    return re.sub(r'(?:^|_)([a-z])', lambda x: x.group(1).upper(), name)

def snake2camelback(name):
    return re.sub(r'_([a-z])', lambda x: x.group(1).upper(), name)

def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])

def camelback2snake(name):
    return re.sub(r'[A-Z]', lambda x: '_' + x.group(0).lower(), name)

class MBMixin:
    last_musicbrainz_sync_time = Column(DateTime, default=func.now())

    @declared_attr
    def __tablename__(cls):
        return camel2snake(cls.__name__)

    # the key to match against in MB server query data
    @declared_attr
    def __mbentity__(cls):
        return camel2snake(cls.__name__).replace('_', '-')

    @classmethod
    def create_from_mbserver(cls, id, includes):
        return cls.create_from_data(cls.server_query(id, includes=includes))

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
            else:
                print("Attribute %s not found in data model of %s. Contents: %s" % (attr_name, cls.__tablename__, data[attr_name]))
        return(c)

    @classmethod
    def data_transformer(cls, data):
        if cls.__mbentity__ in data:
            data = data[cls.__mbentity__]
        return data

release_label_assoc = Table('release_label', Base.metadata,
    Column('label_id', String, ForeignKey('label.id')),
    Column('release_id', String, ForeignKey('release.id')),
    Column('catalog_number', String)
)
release_artist_credit_assoc = Table("release_artist_credit", Base.metadata,
    Column('artist_id', String, ForeignKey('artist.id')),
    Column('release_id', String, ForeignKey('release.id')),
)
release_group_artist_credit_assoc = Table("release_group_artist_credit", Base.metadata,
    Column('artist_id', String, ForeignKey('artist.id')),
    Column('release_group_id', String, ForeignKey('release_group.id')),
)
track_artist_credit_assoc = Table("track_artist_credit", Base.metadata,
    Column('artist_id', String, ForeignKey('artist.id')),
    Column('track_id', String, ForeignKey('track.id')),
)
recording_artist_credit_assoc = Table("recording_artist_credit", Base.metadata,
    Column('artist_id', String, ForeignKey('artist.id')),
    Column('recording_id', String, ForeignKey('recording.id')),
)

class Label(Base, MBMixin):
    id = Column(String, primary_key=True)
    name = Column(String)
    sort_name = Column(String)
    disambiguation = Column(String)
    sort_name = Column(String)
    type = Column(String)
    area_id = Column(String, ForeignKey("area.id"))
    area = relationship("Area", back_populates="label_list")
    life_span_begin = Column(String)
    life_span_end = Column(String)
    life_span_ended = Column(Boolean)
    label_code = Column(String)
    country = Column(String)
    release_count = Column(Integer)
    release_list = relationship("Release", release_label_assoc, back_populates="label_list")

    @staticmethod
    def server_query(id, includes=[]):
        return mb.get_label_by_id(id, includes=includes)['label']

    @classmethod
    def data_transformer(cls, data):
        if cls.__mbentity__ in data:
            data = data[cls.__mbentity__]
        if 'life-span' in data:
            ls = data['life-span']
            if 'ended' in ls:
                data['life-span-ended'] = ls['ended']
            if 'end' in ls:
                data['life-span-end'] = ls['end']
            if 'begin' in ls:
                data['life-span-begin'] = ls['begin']
            data.pop('life-span', None)
        return data

    def __repr__(self):
        return self.name

class Artist(Base, MBMixin):
    id = Column(String, primary_key=True)
    disambiguation = Column(String)
    name = Column(String)
    sort_name = Column(String)
    type = Column(String)
    country = Column(String)
    gender = Column(String)
    area_id = Column(String, ForeignKey("area.id"))
    area = relationship("Area", foreign_keys=[area_id])
    begin_area_id = Column(String, ForeignKey("area.id"))
    begin_area = relationship("Area", foreign_keys=[begin_area_id])
    life_span_begin = Column(String)
    life_span_end = Column(String)
    life_span_ended = Column(Boolean)
    release_count = Column(Integer)
    release_list = relationship("Release", release_artist_credit_assoc, back_populates="artist_credit")
    release_group_count = Column(Integer)
    release_group_list = relationship("ReleaseGroup", release_group_artist_credit_assoc, back_populates="artist_credit")
    track_list = relationship("Track", track_artist_credit_assoc, back_populates="artist_credit")
    recording_count = Column(Integer)
    recording_list = relationship("Recording", recording_artist_credit_assoc, back_populates="artist_credit")

    @classmethod
    def data_transformer(cls, data):
        if cls.__mbentity__ in data:
            data = data[cls.__mbentity__]
        if 'life-span' in data:
            ls = data['life-span']
            if 'ended' in ls:
                data['life-span-ended'] = ls['ended']
            if 'end' in ls:
                data['life-span-end'] = ls['end']
            if 'begin' in ls:
                data['life-span-begin'] = ls['begin']
            data.pop('life-span', None)
        return data

    @staticmethod
    def server_query(id, includes=[]):
        return mb.get_artist_by_id(id, includes=includes)

class Release(Base, MBMixin):
    id = Column(String, primary_key=True)
    disambiguation = Column(String)
    asin = Column(String)
    date = Column(String)
    artist_credit = relationship("Artist", release_artist_credit_assoc, back_populates="release_list")
    artist_credit_phrase = Column(String)
    barcode = Column(String)
    country = Column(String)
    # cover_art_archive = Column(String)
    label_list = relationship("Label", release_label_assoc, back_populates="release_list")
    language = Column(String)
    medium_count = Column(Integer)
    track_count = Column(Integer)
    track_list = relationship("Track", back_populates="release")
    packaging = Column(String)
    quality = Column(String)
    release_event_count = Column(Integer)
    release_event_list = relationship("ReleaseEvent", back_populates="release")
    release_group_id = Column(String, ForeignKey("release_group.id"))
    release_group = relationship("ReleaseGroup", back_populates="release_list")
    cover_art_front = Column(Boolean)
    cover_art_back = Column(Boolean)

    script = Column(String)
    status = Column(String)
    # text_representation = Column(String)
    title = Column(String)

    @classmethod
    def data_transformer(cls, data):
        if len(data) == 1 and cls.__mbentity__ in data:
            data = data[cls.__mbentity__]

        # Add a medium number to every track entry
        if 'medium-list' in data:
            for m in data['medium-list']:
                for t in m['track-list']:
                    t['medium-number'] = m['position']
            # add a "track-list" attribute to the main data
            data['track-list'] = [t for m in data['medium-list'] for t in m['track-list']]
            # add a track-count field
            data['track-count'] = len(data['track-list'])
            # delete the "mediun-list"
            data.pop('medium-list', None)
        if 'text-representation' in data:
            if 'script' in data['text-representation']:
                data['script'] = data['text-representation']['script']
            if 'language' in data['text-representation']:
                data['language'] = data['text-representation']['language']
            data.pop('text-representation', None)

        if 'cover-art-archive' in data:
            data['cover-art-front'] = data['cover-art-archive']['front']
            data['cover-art-back'] = data['cover-art-archive']['back']
            data.pop('cover-art-archive', None)
        return data

    @staticmethod
    def server_query(id, includes=[]):
        data = mb.get_release_by_id(id, includes=includes)
        return data

    def __repr__(self):
        return self.title

class ReleaseGroup(Base, MBMixin):
    id = Column(String, primary_key=True)
    title = Column(String)
    artist_credit = relationship("Artist", release_group_artist_credit_assoc, back_populates="release_group_list")
    artist_credit_phrase = Column(String)
    type = Column(String)
    primary_type = Column(String)
    first_release_date = Column(String)
    secondary_types = Column(String)
    release_list = relationship("Release", back_populates="release_group")

    @classmethod
    def data_transformer(cls, data):
        if 'secondary-type-list' in data:
            data['secondary-types'] = ' '.join(data['secondary-type-list'])
            data.pop('secondary-type-list', None)
        return data

class ReleaseEvent(Base, MBMixin):
    id = Column(Integer, primary_key=True)
    release_id = Column(String, ForeignKey("release.id"))
    date = Column(String)
    area_id = Column(String, ForeignKey("area.id"))
    area = relationship("Area", back_populates="release_event_list")
    release = relationship("Release", back_populates='release_event_list')

class Area(Base, MBMixin):
    id = Column(String, primary_key=True)
    release_event_list = relationship("ReleaseEvent", back_populates="area")
    label_list = relationship("Label", back_populates="area")
    name = Column(String)
    sort_name = Column(String)
    @classmethod
    def data_transformer(cls, data):
        data.pop('iso-3166-1-code-list', None)
        return data

class Track(Base, MBMixin):
    id = Column(String, primary_key=True)
    artist_credit_phrase = Column(String)
    artist_credit = relationship("Artist", track_artist_credit_assoc, back_populates="track_list")
    length = Column(Integer)
    number = Column(String)
    medium_number = Column(String)
    position = Column(Integer)
    recording_id = Column(String, ForeignKey('recording.id'))
    recording = relationship("Recording", back_populates='track_list')
    release_id = Column(String, ForeignKey("release.id"))
    release = relationship("Release", back_populates="track_list")
    title = Column(String)
    track_or_recording_length = Column(Integer)

class Recording(Base, MBMixin):
    id = Column(String, primary_key=True)
    artist_credit_phrase = Column(String)
    artist_credit = relationship("Artist", recording_artist_credit_assoc, back_populates="recording_list")
    length = Column(Integer)
    title = Column(String)
    isrc_count = Column(Integer)
    isrc_list = relationship("ISRC", back_populates='recording')
    track_list = relationship("Track", back_populates='recording')

    @classmethod
    def data_transformer(cls, data):
        if 'isrc-list' in data:
            data['isrc-list'] = [{'id': a} for a in data['isrc-list']]
        return data

class ISRC(Base, MBMixin):
    __tablename__ = 'isrc'
    id = Column(String, primary_key=True)
    recording_id = Column(String, ForeignKey('recording.id'))
    recording = relationship("Recording", back_populates='isrc_list')

Base.metadata.create_all(engine)
label_id = '4d7e2ccf-5379-43bf-8ed3-a02611b00bf2' # Border Community
label_id = '5bda15e5-d721-4f01-bdc1-24ed2f712712' # Trojan Records
label_id = '652d78b4-accd-47a1-907f-3e1bc400b61e' # CAAMA Music
label = Label.create_from_mbserver(label_id, includes=['releases'])
session.merge(label)

for r in session.query(Release).all():
    includes = ['artist-credits', 'artist-rels', 'recording-level-rels', 'isrcs', 'recordings', 'release-groups']
    if r.id:
        new = Release.create_from_mbserver(r.id, includes=includes)
        session.merge(new)

for a in session.query(Artist).all():
    new = Artist.create_from_mbserver(a.id, includes=[])
    session.merge(new)

session.commit()
