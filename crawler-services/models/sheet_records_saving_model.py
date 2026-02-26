from sqlalchemy import Column, Integer, text, Text, Date, String, ForeignKey
from db.base import Base

class SheetRecordsSavingModel(Base):
    __tablename__ = "sheet_records"

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
    
    url = Column(
        Text, 
        nullable=False
    )

    table_name = Column(
        String(255), 
        nullable=True
    )
    
    description = Column(
        Text, 
        nullable=True
    )
    
    created_at = Column(
        Date, 
        server_default=text("CURRENT_DATE")
    )