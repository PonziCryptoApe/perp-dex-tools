import argparse
import sys
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import hashlib
import base64
import getpass

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

def get_secret_key(for_encryption: bool = False) -> str:
    """
    安全获取密钥
    - macOS 下 getpass 默认完全不回显，因此提前给出明确提示
    - 如果是加密操作，要求输入两次进行确认
    """
    print("\033[93m注意：macOS 系统下输入密码时，终端可能不会显示任何字符（包括 *），这是正常现象。\033[0m")
    print("输入完成后直接按回车确认。")

    if for_encryption:
        while True:
            print("请输入加密密钥：")
            key1 = getpass.getpass("密钥: ")
            print("请再次输入相同密钥以确认：")
            key2 = getpass.getpass("确认密钥: ")

            if key1 == key2:
                if len(key1) == 0:
                    print("\033[91m错误：密钥不能为空！\033[0m")
                    continue
                print(f"\n\033[92m密钥确认成功（长度：{len(key1)} 个字符）\033[0m\n")
                return key1
            else:
                print("\033[91m两次输入的密钥不一致，请重新输入。\033[0m\n")
                # 继续循环重新输入
    else:
        # 解密只需输入一次
        print("请输入解密密钥：")
        secret_key = getpass.getpass("密钥: ")
        if len(secret_key) == 0:
            print("\033[91m错误：密钥不能为空！\033[0m")
            sys.exit(1)
        print(f"\n正在使用长度为 {len(secret_key)} 个字符的密钥进行解密...\n")
        return secret_key

if __name__ == "__main__":
    """
    加密
    python helpers/encryption_helper.py encrypt "hello world" -k 123
    解密
    python helpers/encryption_helper.py decrypt "3d49c6b6bdb293178dd7e3f0c0f39442:007ccd7feb5e62d6d68e4af632400789" -k 123
    
    运行时会交互式提示输入密钥（输入时不回显，安全）
    """
    parser = argparse.ArgumentParser(
            description="AES-CBC 加密/解密工具（密钥通过键盘安全输入",
            formatter_class=argparse.RawTextHelpFormatter
        )
    parser.add_argument(
        "action",
        choices=["encrypt", "decrypt", "enc", "dec"],
        help="操作类型：encrypt/enc → 加密，decrypt/dec → 解密"
    )
    parser.add_argument(
        "text",
        help="要加密的明文 或 要解密的密文（格式：iv_hex:encrypted_hex）"
    )

    args = parser.parse_args()
    action = args.action.lower()
    need_confirm = action in ("encrypt", "enc")
    secret_key = get_secret_key(for_encryption=need_confirm)

    helper = EncryptionHelper()
    if action in ("encrypt", "enc"):
        result = helper.encrypt(args.text, secret_key)
        print("加密结果:")
        print(result)
    elif action in ("decrypt", "dec"):
        try:
            result = helper.decrypt(args.text, secret_key)
            print("解密结果:")
            print(result)
        except Exception as e:
            print(f"\033[91m解密失败：{e}\033[0m")