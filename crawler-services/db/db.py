import os
from sqlalchemy import create_engine, select
from sqlalchemy import or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from db.base import Base
from models.lake_saving_model import LakeSavingModel
from models.text_saving_model import TextSavingModel
from models.sheet_records_saving_model import SheetRecordsSavingModel
from constant.type import PageType, URLType

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
    
    def is_url_existed(self, url: str, hash_html: str) -> bool:
        '''
        Checks if a URL exists in the Data Lake and verifies if its content has changed.
        
        Args:
            url (str): The full web address (URL) of the page being checked.
            hash_html (str): The MD5 hash of the current 'live' HTML content.

        Returns:
            bool: 
                - True: If the URL exists AND the hash matches (no changes).
                - False: If the URL is new OR the content has been updated.
        '''
        
        session = self.SessionLocal()
        stmt = select(LakeSavingModel.hash_content).where(LakeSavingModel.url == url)
        stored_hash = session.execute(stmt).scalar_one_or_none()
        
        session.close()

        if stored_hash is None:
            return False
        
        return stored_hash == hash_html

    def add_lake(self, url, page_type, url_type = URLType.UNKNOWN, hash_content=None, status="Pending", title=None, description=None):
        session = self.SessionLocal()
        try:
            stmt = insert(LakeSavingModel).values(
                url=url,
                page_type=page_type,
                url_type=url_type,
                hash_content=hash_content,
                status=status,
                title=title,
                description=description
            )
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['url'],
                set_={
                    "hash_content": stmt.excluded.hash_content,
                    "status": stmt.excluded.status
                },
                where=or_(
                    LakeSavingModel.hash_content != stmt.excluded.hash_content,
                    LakeSavingModel.status != stmt.excluded.status
                )
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
            
    def get_unprocessed_data(self, page_type=None, url_type=None):
        '''
        Fetches records from LakeSavingModel that haven't been processed into TextSavingModel.
    
        Args:
            page_type(str, optional): Filter by PageType enum.
            url_type(str, optional): Filter by URLType enum.
            
        Returns:
            list[dict]: A list of dictionaries representing the unprocessed records.
        '''
        
        if page_type is not None and not isinstance(page_type, PageType):
            raise ValueError(f"page_type must be an instance of PageType Enum, got {type(page_type)}")
        
        if url_type is not None and not isinstance(url_type, URLType):
            raise ValueError(f"url_type must be an instance of URLType Enum, got {type(url_type)}")
        
        session = self.SessionLocal()
        try:
            stmt = (
                select(LakeSavingModel)
                .outerjoin(TextSavingModel, LakeSavingModel.id == TextSavingModel.lake_id)
                .where(TextSavingModel.id == None)
                .where(LakeSavingModel.status == "Success")
            )
            
            if page_type is not None:
                stmt = stmt.where(LakeSavingModel.page_type == page_type)
                
            if url_type is not None:
                stmt = stmt.where(LakeSavingModel.url_type == url_type)
                
            results = session.execute(stmt).scalars().all()
            
            converted_results = [
                {
                    "id": row.id,
                    "url": row.url,
                    "page_type": row.page_type,
                    "url_type": row.url_type,
                    "hash_content": row.hash_content,
                    "status": row.status,
                    "created_at": row.created_at
                }
                for row in results
            ]
            
        except Exception as e:
            print(f'[ERROR][get_unprocessed_data]: Error when query database - {e}')
            
        finally:
            session.close()
        
        return converted_results