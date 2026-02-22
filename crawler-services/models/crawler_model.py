from sqlalchemy import Column, Integer, text, Text, Date, String
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CrawlerModel(Base):
    __tablename__ = "crawler"
    id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )
    url = Column(
        Text,
        nullable=False,
        unique=True,
        index=True
    )
    content = Column(Text, nullable=True)
    crawl_time = Column(Date, server_default=text("CURRENT_DATE"))
    published_date = Column(Date, nullable=True)
    updated_date = Column(Date, nullable=True)
    type = Column(Integer)
