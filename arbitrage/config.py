"""
配置文件 - 集中管理所有配置项
"""

# 交易对配置
SYMBOLS = [
    'BTC-USD-PERP',
    'ETH-USD-PERP',
    'SOL-USD-PERP',
]

# API配置
API_CONFIG = {
    'extended': {
        'base_url': 'https://starknet.app.extended.exchange/api/v1',
        'timeout': 5,
        'rate_limit': 10,  # 每秒请求数限制
    },
    'lighter': {
        'base_url': 'https://mainnet.zklighter.elliot.ai/api/v1',
        'timeout': 5,
        'rate_limit': 10,
    }
}

# 数据收集配置
DATA_CONFIG = {
    'interval_seconds': 5,           # 拉取间隔
    'data_dir': 'data/arbitrage',    # 数据保存目录
    'output_dir': 'output/charts',   # 图表输出目录
    'keep_days': 30,                 # 数据保留天数
}

# 套利策略配置
ARBITRAGE_CONFIG = {
    'min_spread_percentage': 0.1,    # 最小价差阈值（百分比）
    'max_position_size': 1000,       # 最大持仓量（USD）
    'slippage_tolerance': 0.05,      # 滑点容忍度（百分比）
}

# 风控配置
RISK_CONFIG = {
    'max_daily_trades': 100,         # 每日最大交易次数
    'max_daily_loss': 1000,          # 每日最大亏损（USD）
    'stop_loss_percentage': 0.5,     # 止损百分比
}

# 通知配置
NOTIFICATION_CONFIG = {
    'enable_telegram': False,
    'telegram_bot_token': '',
    'telegram_chat_id': '',
    'enable_email': False,
    'email_smtp': '',
}