from cryptography.fernet import Fernet
from app.core.config import settings
import base64

# Generate a key for encryption (in production, store this securely)
def get_encryption_key():
    # Use JWT secret as base for encryption key
    key = base64.urlsafe_b64encode(settings.jwt_secret_key.encode()[:32].ljust(32, b'0'))
    return key

def encrypt_data(data: str) -> str:
    """Encrypt sensitive data"""
    f = Fernet(get_encryption_key())
    encrypted_data = f.encrypt(data.encode())
    return encrypted_data.decode()

def decrypt_data(encrypted_data: str) -> str:
    """Decrypt sensitive data"""
    if encrypted_data is None:
        raise ValueError("Cannot decrypt None value. Credentials not set.")
    try:
        f = Fernet(get_encryption_key())
        decrypted_data = f.decrypt(encrypted_data.encode())
        return decrypted_data.decode()
    except Exception as e:
        raise ValueError(f"Failed to decrypt credentials: {str(e)}")
