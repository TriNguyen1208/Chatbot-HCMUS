import os
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from db.base import Base
from models.lake_saving_model import LakeSavingModel
from models.text_saving_model import TextSavingModel
from models.sheet_records_saving_model import SheetRecordsSavingModel

load_dotenv()

class Database:
    _instance = None

    def _init_connection(self):
        self.db_url = os.getenv("DATABASE_URL")
        if not self.db_url:
            raise ValueError("DATABASE_URL is not existed")
        
        if "sslmode" not in self.db_url:
            self.db_url += "?sslmode=require"

        self.engine = create_engine(self.db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Tạo bảng nếu chưa có
        Base.metadata.create_all(bind=self.engine)
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._init_connection()
        return cls._instance
    
    def check_is_existed_url(self, url):
        session = self.SessionLocal()
        try:
            result = session.query(LakeSavingModel).filter(
                LakeSavingModel.url == url
            ).first()

            return result is not None

        except Exception as e:
            print(f"Error when checking url existence: {e}")
            return False

        finally:
            session.close()

    def add_lake(self, url, page_index, url_type, hash_content=None, status="Pending"):
        session = self.SessionLocal()
        try:
            stmt = insert(LakeSavingModel).values(
                url=url,
                page_index=page_index,
                url_type=url_type,
                hash_content=hash_content,
                status=status
            ).on_conflict_do_nothing(
                index_elements=["url"]
            )
            session.execute(stmt)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"[Error][add_lake]: Saving into lake: {e}")
        finally:
            session.close()
            
    def add_text_warehouse(self, lake_id, content):
        session = self.SessionLocal()
        try:
            stmt = insert(TextSavingModel).values(
                lake_id=lake_id,
                content=content
            )
            session.execute(stmt)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"[Error][add_text_warehouse]: Saving into text warehouse: {e}")
        finally:
            session.close()
            
    def add_sheet_records_warehouse(self, lake_id, url, table_name, description=None):
        session = self.SessionLocal()
        try:
            stmt = insert(SheetRecordsSavingModel).values(
                lake_id=lake_id,
                url=url,
                table_name=table_name,
                description=description
            )
            session.execute(stmt)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"[Error][add_text_warehouse]: Saving into text warehouse: {e}")
        finally:
            session.close()