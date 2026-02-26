from sqlalchemy import Column, Integer, text, Text, Date, ForeignKey
from db.base import Base

class TextSavingModel(Base):
    __tablename__ = "text"

    id = Column(
        Integer, 
        primary_key=True, 
        autoincrement=True
    )
    
    lake_id = Column(
        Integer,
        ForeignKey("lake.id", ondelete="CASCADE"), 
        nullable=False
    )
    
    content = Column(
        Text, 
        nullable=False
    )
    
    created_at = Column(
        Date, 
        server_default=text("CURRENT_DATE")
    )
    
