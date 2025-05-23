from sqlalchemy import JSON, Boolean, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Users(Base):
    __table_args__ = {"schema": "cbx"}
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, nullable=False)
    password = Column(String, nullable=False)
    properties = Column(JSON, nullable=False)
    status = Column(Boolean, nullable=False)
    role = Column(String, nullable=False)
