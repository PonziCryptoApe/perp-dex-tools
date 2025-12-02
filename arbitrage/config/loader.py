"""配置加载器"""

import yaml
from pathlib import Path
from decimal import Decimal
from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass
class PairConfig:
    """交易对配置"""
    pair_id: str              # 交易对 ID
    enabled: bool             # 是否启用
    symbol: str               # 币种
    exchange_a: str           # 交易所 A 名称
    exchange_b: str           # 交易所 B 名称
    quantity: Decimal         # 交易数量
    open_threshold: float     # 开仓阈值（%）
    close_threshold: float    # 平仓阈值（%）
    min_depth_quantity: Decimal  # ✅ 新增：最小深度阈值
    variational_config: Dict[str, Any]  # ✅ 新增：Variational 特定配置


def load_pair_config(pair_id: str) -> PairConfig:
    """
    加载交易对配置
    
    Args:
        pair_id: 交易对 ID（如 extended_lighter_btc）
    
    Returns:
        PairConfig
    
    Raises:
        ValueError: 配置不存在或未启用
        FileNotFoundError: 配置文件不存在
    """
    config_path = Path(__file__).parent / 'pairs.yaml'
    
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    if 'pairs' not in config:
        raise ValueError("配置文件格式错误：缺少 'pairs' 字段")
    
    if pair_id not in config['pairs']:
        available_pairs = ', '.join(config['pairs'].keys())
        raise ValueError(
            f"未找到交易对配置: {pair_id}\n"
            f"可用的交易对: {available_pairs}"
        )
    
    pair_data = config['pairs'][pair_id]
    
    if not pair_data.get('enabled', False):
        raise ValueError(f"交易对未启用: {pair_id}，请在配置文件中设置 enabled: true")
     # ✅ 解析基础配置
    quantity = Decimal(str(pair_data['quantity']))
    
    # ✅ 解析 min_depth_quantity（可选，默认为 quantity 的 10%，最小为 0.001）
    if 'min_depth_quantity' in pair_data:
        min_depth_quantity = Decimal(str(pair_data['min_depth_quantity']))
    else:
        # 默认为交易量的 10%，但最小为 0.001
        default_min_depth = max(quantity * Decimal('0.1'), Decimal('0.0001'))
        min_depth_quantity = default_min_depth

    variational_config = pair_data.get('variational_config', {})

    return PairConfig(
        pair_id=pair_id,
        enabled=pair_data['enabled'],
        symbol=pair_data['symbol'],
        exchange_a=pair_data['exchange_a'],
        exchange_b=pair_data['exchange_b'],
        quantity=Decimal(str(pair_data['quantity'])),
        open_threshold=float(pair_data['open_threshold']),
        close_threshold=float(pair_data['close_threshold']),
        min_depth_quantity=min_depth_quantity,
        variational_config=variational_config
    )

def list_all_pairs() -> list:
    """
    列出所有可用的交易对
    
    Returns:
        交易对 ID 列表
    """
    config_path = Path(__file__).parent / 'pairs.yaml'
    
    if not config_path.exists():
        return []
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    if 'pairs' not in config:
        return []
    
    return list(config['pairs'].keys())

def list_enabled_pairs() -> list:
    """
    列出所有已启用的交易对
    
    Returns:
        已启用的交易对 ID 列表
    """
    config_path = Path(__file__).parent / 'pairs.yaml'
    
    if not config_path.exists():
        return []
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    if 'pairs' not in config:
        return []
    
    return [
        pair_id
        for pair_id, pair_data in config['pairs'].items()
        if pair_data.get('enabled', False)
    ]