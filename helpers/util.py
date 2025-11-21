"""工具函数和辅助类"""

import asyncio
import functools
import logging

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