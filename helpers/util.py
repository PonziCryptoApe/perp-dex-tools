"""工具函数和辅助类"""

import asyncio
import functools
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


class Config:
    """Simple config class to wrap dictionary for Extended client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


def async_retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    异步函数重试装饰器
    
    Args:
        max_attempts: 最大重试次数（包含首次尝试）
        delay: 初始延迟时间（秒）
        backoff: 延迟倍数（指数退避）
        exceptions: 需要重试的异常类型
    
    Example:
        @async_retry(max_attempts=3, delay=0.5, backoff=1.5)
        async def my_function():
            result = await some_api_call()
            if not result.success:
                raise Exception("API call failed")
            return result
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    # 执行函数
                    result = await func(*args, **kwargs)
                    
                    # 成功则返回
                    if attempt > 1:
                        logger.info(
                            f"✅ {func.__name__} 重试成功 (第 {attempt} 次尝试)"
                        )
                    return result
                
                except exceptions as e:
                    last_exception = e
                    
                    # 最后一次尝试失败，不再重试
                    if attempt >= max_attempts:
                        logger.error(
                            f"❌ {func.__name__} 重试失败，已达最大尝试次数 {max_attempts}"
                        )
                        raise
                    
                    # 记录重试信息
                    logger.warning(
                        f"⚠️ {func.__name__} 失败 (第 {attempt}/{max_attempts} 次): {e}"
                    )
                    
                    # 等待后重试
                    logger.info(f"⏳ {current_delay:.1f} 秒后重试...")
                    await asyncio.sleep(current_delay)
                    
                    # 指数退避
                    current_delay *= backoff
            
            # 理论上不会到这里
            raise last_exception
        
        return wrapper
    return decorator


def beijing_to_timestamp(beijing_str: str, format_str: str = '%Y-%m-%d %H:%M:%S') -> float:
    """
    将北京时间字符串转换为 Unix 时间戳。
    
    :param beijing_str: 北京时间字符串，例如 "2025-12-24 10:00:00"
    :param format_str: 时间格式字符串，默认 '%Y-%m-%d %H:%M:%S'
    :return: Unix 时间戳（浮点数，秒）
    """
    # 解析字符串为 datetime 对象（无时区）
    dt = datetime.strptime(beijing_str, format_str)
    
    # 设置北京时区 (UTC+8)
    cst_tz = timezone(timedelta(hours=8))
    dt = dt.replace(tzinfo=cst_tz)
    
    # 获取时间戳（自动转换为 UTC）
    return dt.timestamp()

# 示例使用
if __name__ == "__main__":
    test_time = "2025-12-24 10:00:00"  # 当前日期示例
    timestamp = beijing_to_timestamp(test_time)
    print(f"北京时间 {test_time} 的时间戳: {timestamp}")
    # 输出: 北京时间 2025-12-24 10:00:00 的时间戳: 1766541600.0