import pandas as pd
import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from gspread_dataframe import set_with_dataframe
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
import os

class GoogleSheet:
    _instance = None

    def __init_connection__(self):
        self.folder_id = "1Z6pYH3BLgTHCInY1_eVgInrzJWbyK3IY"
        self.client_secret_path = "client_secret.json"
        self.token_path = "token.json"
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        self.creds = None
        if os.path.exists(self.token_path):
            self.creds = Credentials.from_authorized_user_file(self.token_path, self.scopes)
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.client_secret_path, self.scopes)
                # Chạy server cục bộ để nhận code xác thực từ trình duyệt
                self.creds = flow.run_local_server(port=0)
            with open(self.token_path, 'w') as token:
                token.write(self.creds.to_json())
        self.client = gspread.authorize(self.creds)
        self.drive_service = build('drive', 'v3', credentials=self.creds)

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GoogleSheet, cls).__new__(cls)
            cls._instance.__init_connection__()
        return cls._instance
    
    def save(self, df: pd.DataFrame, title: str):
        """
        Upload pandas DataFrame to a new Google Sheet and return the sheet URL
        
        Args:
            df (pd.DataFrame): dataframe need to upload
            title (str): Title of name file.
        
        Returns:
            str: URL of Google Sheet
        """

        file_metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [self.folder_id] 
        }
        file = self.drive_service.files().create(
            body=file_metadata, 
            fields='id'
        ).execute()

        new_file_id = file.get('id')
        spreadsheet = self.client.open_by_key(new_file_id)
        worksheet = spreadsheet.sheet1
        set_with_dataframe(worksheet, df)

        return spreadsheet.url