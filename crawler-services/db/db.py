from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from models.crawler_model import CrawlerModel, Base
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert

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
            result = session.query(CrawlerModel).filter(
                CrawlerModel.url == url
            ).first()

            return result is not None

        except Exception as e:
            print(f"Error when checking url existence: {e}")
            return False

        finally:
            session.close()

    def add_database(self, url, type, hash_content=None, status="Pending"):
        session = self.SessionLocal()
        try:
            stmt = insert(CrawlerModel).values(
                url=url,
                type=type,
                hash_content=hash_content,
                status=status
            ).on_conflict_do_nothing(
                index_elements=["url"]
            )
            session.execute(stmt)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error when saving into database: {e}")
        finally:
            session.close()