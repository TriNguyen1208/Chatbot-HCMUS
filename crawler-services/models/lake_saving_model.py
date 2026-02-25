from sqlalchemy import Column, Integer, text, Text, Date, String
from db.base import Base

class LakeSavingModel(Base):
    __tablename__ = "lake"
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
    type = Column(Integer)
    hash_content = Column(Text, nullable=True)
    status = Column(String)
    created_at = Column(Date, server_default=text("CURRENT_DATE"))
    
