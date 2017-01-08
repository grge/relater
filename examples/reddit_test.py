from sqlalchemy import Table, Column, ForeignKey, String, DateTime, func, Integer, Boolean
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import relationship, sessionmaker
from relater import Relater
import requests
import requests.auth

Base = declarative_base()

engine = create_engine('sqlite:///out.db')

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

reddit_app_id = ''
reddit_app_secret = ''
reddit_username = ''
reddit_password = ''
reddit_app_ua = 'Relater/0.1'

client_auth = requests.auth.HTTPBasicAuth(reddit_app_id, reddit_app_secret)

ua = 'Relater/0.1'
post_data = {"grant_type": "password", "username": reddit_username, "password": reddit_password} 
headers = {"User-Agent": ua}

response = requests.post("https://www.reddit.com/api/v1/access_token", 
                        auth=client_auth, data=post_data, headers=headers)
access_token = response.json()['access_token']

class User(Base, Relater):
    headers = {"Authorization": "bearer %s" % access_token, "User-Agent": ua}

    name = Column(String, primary_key=True)
    id = Column(String)
    is_friend = Column(Boolean)
    is_employee = Column(Boolean)
    is_suspended = Column(Boolean)
    link_karma = Column(Integer)
    comment_karma = Column(Integer)

    @property
    def api_endpoint(self):
        return "http://oauth.reddit.com/user/%s/about.json" % self.name

    @staticmethod
    def payload_accessor(data):
        return data['data']


Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
u = User()
u.name = reddit_username 
u.load_from_api(session)

session.merge(u)
session.commit()


