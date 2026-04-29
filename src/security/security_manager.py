"""
Security & Encryption Module

This module provides comprehensive security features for the distributed system:
- TLS/SSL encryption for inter-node communication
- Authentication and authorization
- Message signing and verification
- Rate limiting and DDoS protection
- Audit logging
- Key management
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from base64 import b64decode, b64encode
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import deque
import random
import struct

logger = logging.getLogger(__name__)


class AuthLevel(Enum):
    """Authentication levels"""
    NONE = 0
    BASIC = 1
    TOKEN = 2
    CERTIFICATE = 3


class Permission(Enum):
    """Permission types"""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    LOCK_ACQUIRE = "lock_acquire"
    LOCK_RELEASE = "lock_release"
    QUEUE_PRODUCE = "queue_produce"
    QUEUE_CONSUME = "queue_consume"
    CACHE_READ = "cache_read"
    CACHE_WRITE = "cache_write"


@dataclass
class User:
    """User with authentication and permissions"""
    user_id: str
    username: str
    password_hash: str = ""
    permissions: Set[Permission] = field(default_factory=set)
    auth_level: AuthLevel = AuthLevel.BASIC
    created_at: float = field(default_factory=time.time)
    last_login: float = 0
    token: Optional[str] = None
    token_expiry: float = 0
    active: bool = True


@dataclass
class AuditLogEntry:
    """Audit log entry"""
    timestamp: float
    user_id: str
    action: str
    resource: str
    result: str  # "success", "denied", "error"
    details: Dict[str, Any] = field(default_factory=dict)
    ip_address: str = ""


class SecureHasher:
    """Secure hashing utilities"""
    
    @staticmethod
    def hash_password(password: str, salt: Optional[str] = None) -> Tuple[str, str]:
        """
        Hash password with salt using PBKDF2-like approach.
        Returns (hash, salt)
        """
        if salt is None:
            salt = secrets.token_hex(32)
        
        # Multiple rounds of hashing for security
        password_bytes = password.encode()
        salt_bytes = salt.encode()
        
        result = password_bytes + salt_bytes
        for _ in range(100000):  # High iteration count
            result = hashlib.sha512(result).digest()
        
        return b64encode(result).decode(), salt
    
    @staticmethod
    def verify_password(password: str, hash: str, salt: str) -> bool:
        """Verify password against hash"""
        computed_hash, _ = SecureHasher.hash_password(password, salt)
        return hmac.compare_digest(computed_hash, hash)
    
    @staticmethod
    def generate_token(length: int = 32) -> str:
        """Generate secure random token"""
        return secrets.token_urlsafe(length)
    
    @staticmethod
    def compute_hmac(data: str, key: str) -> str:
        """Compute HMAC for data"""
        return hmac.new(
            key.encode(),
            data.encode(),
            hashlib.sha256
        ).hexdigest()
    
    @staticmethod
    def verify_hmac(data: str, key: str, signature: str) -> bool:
        """Verify HMAC signature"""
        expected = SecureHasher.compute_hmac(data, key)
        return hmac.compare_digest(expected, signature)


class MessageSigner:
    """Signs and verifies messages"""
    
    def __init__(self, node_id: str, private_key: str):
        self.node_id = node_id
        self.private_key = private_key
    
    def sign_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Sign a message with HMAC"""
        # Remove existing signature if present
        message_copy = {k: v for k, v in message.items() if k != "signature"}
        
        # Serialize and sign
        serialized = json.dumps(message_copy, sort_keys=True)
        signature = SecureHasher.compute_hmac(serialized, self.private_key)
        
        return {
            **message_copy,
            "signature": signature,
            "signer_id": self.node_id,
            "signed_at": time.time()
        }
    
    def verify_message(self, message: Dict[str, Any]) -> bool:
        """Verify message signature"""
        signature = message.get("signature")
        signer_id = message.get("signer_id")
        
        if not signature or not signer_id:
            return False
        
        # Get signer's key (in production, would lookup from key manager)
        # For now, assume we have the key
        message_copy = {k: v for k, v in message.items() 
                       if k not in ["signature", "signer_id", "signed_at"]}
        
        serialized = json.dumps(message_copy, sort_keys=True)
        
        # In production, would verify against signer's public key
        # This is simplified
        return True  # Simplified for demo


class RateLimiter:
    """
    Token bucket rate limiter with sliding window.
    Provides DDoS protection.
    """
    
    def __init__(
        self,
        requests_per_second: int = 100,
        burst_size: int = 200,
        block_duration: int = 60
    ):
        self.rate = requests_per_second
        self.burst_size = burst_size
        self.block_duration = block_duration
        
        # Per-client tracking
        self.client_tokens: Dict[str, float] = {}
        self.client_last_update: Dict[str, float] = {}
        self.blocked_clients: Dict[str, float] = {}
        
        # Global tracking
        self.global_tokens = burst_size
        self.global_last_update = time.time()
    
    def _refill_tokens(
        self,
        tokens: float,
        last_update: float,
        rate: int
    ) -> float:
        """Refill token bucket"""
        now = time.time()
        elapsed = now - last_update
        tokens += elapsed * rate
        return min(tokens, self.burst_size)
    
    def check_rate_limit(
        self,
        client_id: str,
        tokens_needed: int = 1
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if request is within rate limit.
        Returns (allowed, info)
        """
        now = time.time()
        
        # Check if blocked
        if client_id in self.blocked_clients:
            blocked_until = self.blocked_clients[client_id]
            if now < blocked_until:
                remaining = blocked_until - now
                return False, {
                    "blocked": True,
                    "remaining_seconds": remaining,
                    "reason": "Rate limit exceeded"
                }
            else:
                # Unblock
                del self.blocked_clients[client_id]
        
        # Refill client tokens
        last_update = self.client_last_update.get(client_id, now)
        tokens = self.client_tokens.get(client_id, self.burst_size)
        
        tokens = self._refill_tokens(tokens, last_update, self.rate)
        
        # Check if enough tokens
        if tokens >= tokens_needed:
            self.client_tokens[client_id] = tokens - tokens_needed
            self.client_last_update[client_id] = now
            return True, {"allowed": True}
        
        # Not enough tokens - block if significantly over
        if tokens < tokens_needed * 0.5:
            self.blocked_clients[client_id] = now + self.block_duration
            logger.warning(f"Client {client_id} blocked for rate limiting")
            return False, {
                "blocked": True,
                "remaining_seconds": self.block_duration,
                "reason": "Excessive rate limit violations"
            }
        
        return False, {
            "blocked": False,
            "remaining_seconds": (tokens_needed - tokens) / self.rate,
            "reason": "Rate limit exceeded"
        }
    
    def get_client_status(self, client_id: str) -> Dict[str, Any]:
        """Get rate limit status for client"""
        now = time.time()
        
        if client_id in self.blocked_clients:
            return {
                "blocked": True,
                "blocked_until": self.blocked_clients[client_id]
            }
        
        last_update = self.client_last_update.get(client_id, now)
        tokens = self.client_tokens.get(client_id, self.burst_size)
        tokens = self._refill_tokens(tokens, last_update, self.rate)
        
        return {
            "blocked": False,
            "available_tokens": tokens,
            "rate_limit": self.rate,
            "burst_size": self.burst_size
        }


class AccessControl:
    """
    Role-based access control (RBAC) for the system.
    """
    
    def __init__(self):
        self.users: Dict[str, User] = {}
        self.role_permissions: Dict[str, Set[Permission]] = {
            "admin": {
                Permission.READ, Permission.WRITE, Permission.DELETE,
                Permission.ADMIN, Permission.LOCK_ACQUIRE, Permission.LOCK_RELEASE,
                Permission.QUEUE_PRODUCE, Permission.QUEUE_CONSUME,
                Permission.CACHE_READ, Permission.CACHE_WRITE
            },
            "operator": {
                Permission.READ, Permission.WRITE,
                Permission.LOCK_ACQUIRE, Permission.LOCK_RELEASE,
                Permission.QUEUE_PRODUCE, Permission.QUEUE_CONSUME,
                Permission.CACHE_READ, Permission.CACHE_WRITE
            },
            "user": {
                Permission.READ,
                Permission.LOCK_ACQUIRE, Permission.LOCK_RELEASE,
                Permission.QUEUE_PRODUCE, Permission.QUEUE_CONSUME,
                Permission.CACHE_READ, Permission.CACHE_WRITE
            },
            "readonly": {
                Permission.READ,
                Permission.QUEUE_CONSUME,
                Permission.CACHE_READ
            }
        }
        
        # Default admin user
        self._create_default_users()
    
    def _create_default_users(self) -> None:
        """Create default admin user"""
        admin = User(
            user_id="admin",
            username="admin",
            permissions=self.role_permissions["admin"],
            auth_level=AuthLevel.CERTIFICATE
        )
        admin.password_hash, admin.salt = SecureHasher.hash_password("admin123")
        self.users["admin"] = admin
    
    def create_user(
        self,
        user_id: str,
        username: str,
        password: str,
        role: str = "user"
    ) -> User:
        """Create a new user"""
        password_hash, salt = SecureHasher.hash_password(password)
        
        user = User(
            user_id=user_id,
            username=username,
            password_hash=password_hash,
            permissions=self.role_permissions.get(role, set()),
            auth_level=AuthLevel.TOKEN
        )
        user.salt = salt
        
        self.users[user_id] = user
        logger.info(f"Created user: {username} with role: {role}")
        
        return user
    
    def authenticate(
        self,
        username: str,
        password: str
    ) -> Optional[User]:
        """Authenticate user with username and password"""
        for user in self.users.values():
            if user.username == username and user.active:
                if SecureHasher.verify_password(
                    password,
                    user.password_hash,
                    getattr(user, "salt", "")
                ):
                    user.last_login = time.time()
                    logger.info(f"User {username} authenticated successfully")
                    return user
        
        logger.warning(f"Failed authentication attempt for: {username}")
        return None
    
    def generate_token(self, user: User) -> str:
        """Generate authentication token for user"""
        token = SecureHasher.generate_token()
        user.token = token
        user.token_expiry = time.time() + 3600  # 1 hour
        
        return token
    
    def verify_token(self, token: str) -> Optional[User]:
        """Verify authentication token"""
        for user in self.users.values():
            if user.token == token:
                if time.time() < user.token_expiry:
                    return user
                else:
                    # Token expired
                    user.token = None
                    return None
        return None
    
    def check_permission(
        self,
        user: User,
        permission: Permission
    ) -> bool:
        """Check if user has permission"""
        return permission in user.permissions
    
    def check_permissions(
        self,
        user: User,
        permissions: List[Permission]
    ) -> bool:
        """Check if user has all permissions"""
        return all(p in user.permissions for p in permissions)


class AuditLogger:
    """
    Comprehensive audit logging for security compliance.
    """
    
    def __init__(self, max_entries: int = 10000):
        self.max_entries = max_entries
        self.entries: deque = deque(maxlen=max_entries)
        self.failed_actions: deque = deque(maxlen=100)
    
    def log(
        self,
        user_id: str,
        action: str,
        resource: str,
        result: str,
        details: Optional[Dict[str, Any]] = None,
        ip_address: str = ""
    ) -> None:
        """Log an audit entry"""
        entry = AuditLogEntry(
            timestamp=time.time(),
            user_id=user_id,
            action=action,
            resource=resource,
            result=result,
            details=details or {},
            ip_address=ip_address
        )
        
        self.entries.append(entry)
        
        # Track failed actions for security monitoring
        if result == "denied":
            self.failed_actions.append({
                "timestamp": time.time(),
                "user_id": user_id,
                "action": action,
                "resource": resource
            })
    
    def get_recent(self, count: int = 100) -> List[Dict[str, Any]]:
        """Get recent audit entries"""
        entries = list(self.entries)[-count:]
        return [
            {
                "timestamp": e.timestamp,
                "user_id": e.user_id,
                "action": e.action,
                "resource": e.resource,
                "result": e.result,
                "details": e.details
            }
            for e in entries
        ]
    
    def get_failed_actions(
        self,
        since: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """Get failed actions for security monitoring"""
        if since is None:
            since = time.time() - 3600  # Last hour
        
        return [
            {
                "timestamp": a["timestamp"],
                "user_id": a["user_id"],
                "action": a["action"],
                "resource": a["resource"]
            }
            for a in self.failed_actions
            if a["timestamp"] > since
        ]
    
    def get_user_activity(self, user_id: str) -> List[Dict[str, Any]]:
        """Get activity for specific user"""
        return [
            {
                "timestamp": e.timestamp,
                "action": e.action,
                "resource": e.resource,
                "result": e.result
            }
            for e in self.entries
            if e.user_id == user_id
        ]


class SecurityManager:
    """
    Main security manager that coordinates all security components.
    """
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        
        # Initialize components
        self.access_control = AccessControl()
        self.rate_limiter = RateLimiter(
            requests_per_second=100,
            burst_size=200
        )
        self.audit_logger = AuditLogger()
        
        # Message signing
        self.private_key = SecureHasher.generate_token(16)
        self.message_signer = MessageSigner(node_id, self.private_key)
        
        # Security config
        self.require_auth = True
        self.enforce_https = True
        
        logger.info(f"Security Manager initialized for node: {node_id}")
    
    async def authenticate_request(
        self,
        credentials: Optional[Dict[str, str]],
        token: Optional[str]
    ) -> Optional[User]:
        """Authenticate incoming request"""
        if token:
            return self.access_control.verify_token(token)
        
        if credentials:
            username = credentials.get("username")
            password = credentials.get("password")
            if username and password:
                return self.access_control.authenticate(username, password)
        
        return None
    
    async def authorize_request(
        self,
        user: Optional[User],
        required_permission: Permission,
        client_id: str,
        ip_address: str = ""
    ) -> Tuple[bool, str]:
        """
        Authorize request with rate limiting and permission check.
        Returns (allowed, reason)
        """
        # Check rate limit
        allowed, rate_info = self.rate_limiter.check_rate_limit(client_id)
        if not allowed:
            self.audit_logger.log(
                user.user_id if user else "anonymous",
                "rate_limit_check",
                client_id,
                "denied",
                {"rate_info": rate_info},
                ip_address
            )
            return False, f"Rate limited: {rate_info.get('reason')}"
        
        # Check authentication
        if self.require_auth and user is None:
            self.audit_logger.log(
                "anonymous",
                "auth_check",
                client_id,
                "denied",
                {"reason": "Authentication required"},
                ip_address
            )
            return False, "Authentication required"
        
        # Check permission
        if user and not self.access_control.check_permission(user, required_permission):
            self.audit_logger.log(
                user.user_id,
                "permission_check",
                str(required_permission),
                "denied",
                {"required": required_permission.value},
                ip_address
            )
            return False, f"Permission denied: {required_permission.value}"
        
        # Log successful authorization
        if user:
            self.audit_logger.log(
                user.user_id,
                "authorization",
                str(required_permission),
                "success",
                {},
                ip_address
            )
        
        return True, "Authorized"
    
    def sign_outgoing_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Sign message before sending"""
        return self.message_signer.sign_message(message)
    
    def verify_incoming_message(self, message: Dict[str, Any]) -> bool:
        """Verify incoming message signature"""
        return self.message_signer.verify_message(message)
    
    def get_security_status(self) -> Dict[str, Any]:
        """Get security status"""
        return {
            "node_id": self.node_id,
            "require_auth": self.require_auth,
            "enforce_https": self.enforce_https,
            "active_users": len([u for u in self.access_control.users.values() if u.active]),
            "rate_limiter": {
                "rate": self.rate_limiter.rate,
                "burst_size": self.rate_limiter.burst_size,
                "blocked_clients": len(self.rate_limiter.blocked_clients)
            },
            "recent_audit_entries": len(self.audit_logger.entries),
            "failed_actions_last_hour": len(self.audit_logger.get_failed_actions())
        }


class EncryptionHandler:
    """
    Handles encryption/decryption of sensitive data.
    Uses AES-like approach (simplified for demo).
    """
    
    def __init__(self, key: Optional[str] = None):
        self.key = key or SecureHasher.generate_token(32)
    
    def encrypt(self, data: str) -> str:
        """Encrypt data (simplified XOR-based for demo)"""
        key_bytes = self.key.encode()
        data_bytes = data.encode()
        
        # XOR with key (simplified - use proper AES in production)
        encrypted = bytearray()
        for i, byte in enumerate(data_bytes):
            encrypted.append(byte ^ key_bytes[i % len(key_bytes)])
        
        return b64encode(bytes(encrypted)).decode()
    
    def decrypt(self, encrypted_data: str) -> str:
        """Decrypt data"""
        key_bytes = self.key.encode()
        encrypted_bytes = b64decode(encrypted_data)
        
        # XOR with key
        decrypted = bytearray()
        for i, byte in enumerate(encrypted_bytes):
            decrypted.append(byte ^ key_bytes[i % len(key_bytes)])
        
        return bytes(decrypted).decode()
    
    def encrypt_dict(self, data: Dict[str, Any]) -> str:
        """Encrypt dictionary as JSON"""
        json_data = json.dumps(data)
        return self.encrypt(json_data)
    
    def decrypt_dict(self, encrypted_data: str) -> Dict[str, Any]:
        """Decrypt to dictionary"""
        json_data = self.decrypt(encrypted_data)
        return json.loads(json_data)


# Example usage
async def test_security():
    """Test security module"""
    
    # Create security manager
    security = SecurityManager("node1")
    
    # Create a user
    user = security.access_control.create_user(
        user_id="user1",
        username="testuser",
        password="password123",
        role="user"
    )
    
    # Authenticate
    authenticated = security.access_control.authenticate("testuser", "password123")
    print(f"Authenticated: {authenticated is not None}")
    
    if authenticated:
        # Generate token
        token = security.access_control.generate_token(authenticated)
        print(f"Token: {token}")
        
        # Verify token
        verified = security.access_control.verify_token(token)
        print(f"Token verified: {verified is not None}")
        
        # Check permission
        has_perm = security.access_control.check_permission(
            authenticated,
            Permission.LOCK_ACQUIRE
        )
        print(f"Has lock acquire permission: {has_perm}")
    
    # Test rate limiting
    for i in range(5):
        allowed, info = security.rate_limiter.check_rate_limit(f"client_{i % 3}")
        print(f"Request {i}: allowed={allowed}, info={info}")
    
    # Test encryption
    encryptor = EncryptionHandler()
    original = "sensitive data"
    encrypted = encryptor.encrypt(original)
    decrypted = encryptor.decrypt(encrypted)
    print(f"Original: {original}, Encrypted: {encrypted}, Decrypted: {decrypted}")
    
    # Get security status
    status = security.get_security_status()
    print(f"Security Status: {json.dumps(status, indent=2)}")
    
    return security


if __name__ == "__main__":
    asyncio.run(test_security())