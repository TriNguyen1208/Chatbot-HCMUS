import hashlib
import fitz

def generate_hash_content(content: str) -> str:
    '''
    Generates a consistent MD5 hash for various content types.
    
    Args:
        content (str | bytes | any): The raw content to be hashed.

    Returns:
        str: A 32-character hexadecimal MD5 hash string.
        
        None: If the input content is None.
    '''
    if content is None:
        return None

    hasher = hashlib.md5()
    
    if isinstance(content, str):
        hasher.update(content.encode('utf-8'))
    elif isinstance(content, bytes):
        hasher.update(content)
    else:
        hasher.update(str(content).encode('utf-8'))
        
    return hasher.hexdigest()


def is_pdf_scanned(pdf_path: str) -> bool:
    '''
    Determines if a PDF file is a scanned image or contains extractable text.

    This function attempts to read the first few pages of a PDF to check for 
    the presence of text characters. If no text is found, the PDF is 
    categorized as 'scanned' (an image)
    
    Args:
        pdf_path (str): The file path to the PDF document.

    Returns:
        bool: 
            - True: If no text is detected (likely a scanned image).
            - False: If extractable text is found (text-based PDF).
    '''
    
    try:
        doc = fitz.open(pdf_path)
        
        for i in range(min(len(doc), 5)):
            page = doc.load_page(i)
            
            if page.get_text().strip():
                return False
            
        return True
    except Exception as e:
        print(f"[ERROR][is_pdf_scanned]: cannot read PDF with path {pdf_path}: {e}")
        return True
    finally:
        doc.close()