from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import hashlib
import base64
class EncryptionHelper:
    @staticmethod
    def _mangle_key_exactly_like_buggy_ts(secret_key: str) -> bytes:
        """
        crypto.createHash('sha256').update(String(secretKey)).digest('base64').slice(0, 32)
        """
        # 1. 先 SHA256
        sha256_hash = hashlib.sha256(str(secret_key).encode('utf-8')).digest()
        # 2. 转成 Base64 字符串（Node.js 默认不带 padding）
        base64_str = base64.b64encode(sha256_hash).decode('utf-8').rstrip('=')
        # 3. 截取前 32 个字符（注意：这是 ASCII 字符，不是字节！）
        sliced = base64_str[:32]
        # 4. 把这 32 个字符当成 latin1 编码转成 bytes（Node.js Buffer 默认行为）
        #     Buffer.from(str) 在 Node.js 里默认是 latin1
        key_bytes = sliced.encode('latin1')
        # 注意：这时候 key_bytes 长度大概率不是 32 字节了！可能 24~32 之间
        # 但 AES-256 必须 32 字节，所以后面 Cipher 创建时会报错或自动 padding（取决于环境）
        # 为了完全一致，我们直接返回这个“残缺”的 key
        return key_bytes
    def encrypt(self, text: str, secret_key: str) -> str:
        if not text:
            return text
        # 关键：用完全一样的“残缺 key”生成方式
        key = self._mangle_key_exactly_like_buggy_ts(secret_key)
        iv = get_random_bytes(16)
        # 注意！这里可能会因为 key 长度不是 32 字节而报错
        # 但原 TS 代码在某些 Node.js 版本里居然能跑（内部自动零填充或截断）
        # pycryptodome 严格检查长度，所以我们强制补零到 32 字节（和大部分 Node.js 实际行为一致）
        if len(key) < 32:
            key = key.ljust(32, b'\x00')          # 右边补 0
        elif len(key) > 32:
            key = key[:32]                        # 超长截断
        cipher = AES.new(key, AES.MODE_CBC, iv)
        # PKCS7 填充（和 Node.js crypto 默认一样）
        text_bytes = text.encode('utf-8')
        pad_len = 16 - len(text_bytes) % 16
        padded = text_bytes + bytes([pad_len]) * pad_len
        encrypted = cipher.encrypt(padded)
        return iv.hex() + ':' + encrypted.hex()
    def decrypt(self, text: str, secret_key: str) -> str:
        if not text:
            return ''
        key = self._mangle_key_exactly_like_buggy_ts(secret_key)
        if len(key) < 32:
            key = key.ljust(32, b'\x00')
        elif len(key) > 32:
            key = key[:32]
        parts = text.split(':')
        if len(parts) != 2:
            raise ValueError("Invalid encrypted format")
        iv = bytes.fromhex(parts[0])
        encrypted = bytes.fromhex(parts[1])
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted_padded = cipher.decrypt(encrypted)
        # 去 PKCS7 填充
        pad_len = decrypted_padded[-1]
        if not (1 <= pad_len <= 16):
            raise ValueError("Invalid padding")
        decrypted = decrypted_padded[:-pad_len]
        return decrypted.decode('utf-8')