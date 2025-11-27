"""日志系统工具"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler


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
    log_format = logging.Formatter(
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