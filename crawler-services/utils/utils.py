import hashlib

def generate_hash_content(content)->str:
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