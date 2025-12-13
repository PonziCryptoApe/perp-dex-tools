"""日志系统工具"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler
import time


# ✅ 新增：自定义时区转换器
class BeijingFormatter(logging.Formatter):
    """使用北京时间的日志格式化器"""
    
    converter = time.gmtime  # 先用 GMT
    
    def formatTime(self, record, datefmt=None):
        """覆盖时间格式化方法，转换为北京时间（UTC+8）"""
        # 获取 UTC 时间戳
        ct = self.converter(record.created)
        
        # 转换为北京时间（UTC+8）
        import datetime as dt
        utc_time = dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc)
        beijing_time = utc_time.astimezone(dt.timezone(dt.timedelta(hours=8)))
        
        if datefmt:
            s = beijing_time.strftime(datefmt)
        else:
            s = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        
        return s
    
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
    log_file = log_dir / f"arbitrage_{pair}_{datetime.now().strftime('%Y%m%d')}.log"
    
    # 创建日志格式
    log_format = BeijingFormatter(
        '%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # ✅ 文件 Handler（轮转日志，最大 50MB，保留 10 个备份）
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=10,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_format)
    
    # ✅ 控制台 Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    
    # ✅ 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
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