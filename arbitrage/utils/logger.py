"""日志系统工具"""
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
import os

# ✅ 新增：自定义时区转换器
class ZoneFormatter(logging.Formatter):
    """使用北京时间的日志格式化器"""
    
    converter = lambda *args: datetime.now(timezone(timedelta(hours=8))).timetuple()
    
    def formatTime(self, record, datefmt=None):
        """覆盖时间格式化方法，确保使用北京时间"""
        hours = 0 if os.getenv('TIME_ZONE') == 'UTC' else 8
        dt = datetime.fromtimestamp(record.created, tz=timezone(timedelta(hours=hours)))
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            s = dt.strftime("%Y-%m-%d %H:%M:%S")
        # ✅ 修复：将 record.msecs 转换为整数再格式化
        return f"{s}.{int(record.msecs):03d}"


class NoiseFilter(logging.Filter):
    """过滤已知第三方库噪声日志，避免淹没策略主日志。"""

    BLOCKED_MESSAGE_PARTS = (
        "ProxyManager initialized with",
        "Rotated TLS fingerprint:",
        "Switched to behavior profile:",
        "Applied spoofing level:",
        "Applied timing multiplier:",
        "Applied ML strategy:",
        "Recorded ML attempt:",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not any(part in message for part in self.BLOCKED_MESSAGE_PARTS)
    
def setup_logging(pair: str, log_dir: Path) -> logging.Logger:
    """
    设置日志系统
    
    Args:
        pair: 交易对（如 ETH, BTC）
        log_dir: 日志目录路径
    
    Returns:
        配置好的 logger 实例
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 日志文件路径（按日期 + 交易对命名）
    log_file = log_dir / f"arbitrage_{pair}.log"
    
    # 创建日志格式
    log_format = ZoneFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # ✅ 文件 Handler（轮转日志，最大 50MB，保留 10 个备份）
    file_handler = TimedRotatingFileHandler(
        log_file,
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8',
        utc=True
    )
    file_handler.suffix = "%Y%m%d"
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_format)
    file_handler.addFilter(NoiseFilter())
    
    # ✅ 控制台 Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(log_format)
    console_handler.addFilter(NoiseFilter())
    
    # ✅ 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # 清除现有 handlers（避免重复）
    root_logger.handlers.clear()
    
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # ✅ 抑制第三方库的冗余日志
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('websocket').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info(f"📁 日志文件: {log_file}")
    logger.info(f"📊 CSV 记录: {log_dir / f'arbitrage_{pair}_trades.csv'}")
    logger.info("=" * 60)
    
    return logger
