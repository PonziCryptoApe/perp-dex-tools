#!/bin/bash

SYMBOL="${1:-BTC}"
DURATION="${2:-30}"
OUTPUT_DIR="output/latency_test"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$OUTPUT_DIR"

EXTENDED_LOG="${OUTPUT_DIR}/extended_${SYMBOL}_${TIMESTAMP}.log"
LIGHTER_LOG="${OUTPUT_DIR}/lighter_${SYMBOL}_${TIMESTAMP}.log"
REPORT="${OUTPUT_DIR}/latency_report_${SYMBOL}_${TIMESTAMP}.txt"

echo "=========================================="
echo "WebSocket 延迟测试"
echo "=========================================="
echo "交易对: ${SYMBOL}"
echo "测试时长: ${DURATION} 秒"
echo ""

> "$EXTENDED_LOG"
> "$LIGHTER_LOG"

python3 <<EOF
import asyncio
import websockets
import json
import time

SYMBOL = "${SYMBOL}"
DURATION = ${DURATION}
EXTENDED_LOG = "${EXTENDED_LOG}"
LIGHTER_LOG = "${LIGHTER_LOG}"

def normalize_timestamp(ts, recv_time_ms):
    """标准化时间戳到毫秒"""
    if ts == 0:
        return 0
    
    if ts > 1e15:  # 微秒
        return ts / 1000
    elif ts > 1e12:  # 毫秒
        return ts
    elif ts > 1e9:   # 秒
        return ts * 1000
    else:
        return ts

async def test_extended():
    url = f"wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks/{SYMBOL}-USD?depth=1"
    
    print(f"🔌 Extended: {url}")
    
    start_time = time.time()
    msg_count = 0
    first_msg = True
    
    try:
        async with websockets.connect(url, ping_interval=20, open_timeout=10) as ws:
            print(f"✅ Extended 已连接")
            
            while time.time() - start_time < DURATION:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    recv_time = time.time() * 1000
                    
                    data = json.loads(msg)
                    msg_type = data.get("type", "")
                    
                    if msg_type == "SNAPSHOT":
                        server_ts_raw = data.get("ts", 0)
                        
                        if server_ts_raw > 0:
                            server_ts = normalize_timestamp(server_ts_raw, recv_time)
                            latency = recv_time - server_ts
                            
                            if first_msg:
                                print(f"   首条延迟: {latency:.1f}ms")
                                first_msg = False
                            
                            msg_count += 1
                            
                            with open(EXTENDED_LOG, 'a') as f:
                                f.write(f"{recv_time},{server_ts},{latency}\n")
                            
                            if msg_count % 50 == 0:
                                print(f"Extended: {msg_count} 条")
                    
                    elif msg_type == "PING":
                        await ws.send(json.dumps({"type": "PONG"}))
                
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"⚠️ Extended: {e}")
                    break
    
    except Exception as e:
        print(f"❌ Extended 失败: {type(e).__name__}: {e}")
    
    print(f"{'✅' if msg_count > 0 else '⚠️'} Extended: {msg_count} 条")

async def test_lighter():
    import requests
    
    try:
        resp = requests.get("https://mainnet.zklighter.elliot.ai/api/v1/orderBooks", timeout=10)
        data = resp.json()
        
        market_id = None
        for m in data.get("order_books", []):
            if m["symbol"] == SYMBOL:
                market_id = m["market_id"]
                break
        
        print(f"\n🔍 查找目标: '{SYMBOL}'")
        
        if market_id is None:
            print(f"❌ 未找到 {SYMBOL}")
            return
        
        print(f"✅ market_id: {market_id}")
    except Exception as e:
        print(f"❌ 获取 market_id: {e}")
        return
    
    url = "wss://mainnet.zklighter.elliot.ai/stream"
    print(f"🔌 Lighter: {url}")
    
    start_time = time.time()
    msg_count = 0
    first_msg = True
    
    try:
        async with websockets.connect(url, ping_interval=20) as ws:
            await ws.send(json.dumps({
                "type": "subscribe",
                "channel": f"order_book/{market_id}"
            }))
            
            print(f"✅ Lighter 已连接")
            
            while time.time() - start_time < DURATION:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    recv_time = time.time() * 1000
                    
                    data = json.loads(msg)
                    msg_type = data.get("type", "")
                    
                    if msg_type in ["subscribed/order_book", "update/order_book"]:
                        server_ts_raw = data.get("timestamp", 0)
                        
                        if server_ts_raw > 0:
                            server_ts = normalize_timestamp(server_ts_raw, recv_time)
                            latency = recv_time - server_ts
                            
                            if first_msg:
                                print(f"   首条延迟: {latency:.1f}ms")
                                first_msg = False
                            
                            msg_count += 1
                            
                            with open(LIGHTER_LOG, 'a') as f:
                                f.write(f"{recv_time},{server_ts},{latency}\n")
                            
                            if msg_count % 50 == 0:
                                print(f"Lighter: {msg_count} 条")
                    
                    elif msg_type == "ping":
                        await ws.send(json.dumps({"type": "pong"}))
                
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"⚠️ Lighter: {e}")
                    break
    
    except Exception as e:
        print(f"❌ Lighter 失败: {e}")
    
    print(f"{'✅' if msg_count > 0 else '⚠️'} Lighter: {msg_count} 条")

async def main():
    await asyncio.gather(test_extended(), test_lighter(), return_exceptions=True)

asyncio.run(main())
EOF

echo ""
echo "=========================================="
echo "生成报告"
echo "=========================================="

python3 <<EOF
import statistics

def analyze(f):
    lat = []
    try:
        for line in open(f):
            parts = line.strip().split(',')
            if len(parts) >= 3:
                lat.append(float(parts[2]))
    except:
        return None
    
    if not lat:
        return None
    
    s = sorted(lat)
    
    # 计算抖动（相邻延迟的差值）
    jitter = []
    for i in range(1, len(lat)):
        jitter.append(abs(lat[i] - lat[i-1]))
    
    # 计算时钟偏移估算（假设最小物理延迟 0.3ms）
    min_physical_latency = 0.3
    clock_offset_estimate = statistics.median(lat) - min_physical_latency
    
    return {
        'n': len(lat),
        'mean': statistics.mean(lat),
        'med': statistics.median(lat),
        'min': min(lat),
        'max': max(lat),
        'std': statistics.stdev(lat) if len(lat) > 1 else 0,
        'p5': s[int(len(s)*0.05)],
        'p95': s[int(len(s)*0.95)],
        'p99': s[int(len(s)*0.99)],
        'jitter_mean': statistics.mean(jitter) if jitter else 0,
        'jitter_max': max(jitter) if jitter else 0,
        'jitter_p95': sorted(jitter)[int(len(jitter)*0.95)] if jitter else 0,
        'range': max(lat) - min(lat),
        'clock_offset': clock_offset_estimate,
        'true_latency_estimate': statistics.median(lat) - clock_offset_estimate
    }

ext = analyze("${EXTENDED_LOG}")
lgt = analyze("${LIGHTER_LOG}")

with open("${REPORT}", 'w') as f:
    f.write("=" * 70 + "\n")
    f.write("WebSocket 延迟测试 (考虑时钟偏移)\n")
    f.write("=" * 70 + "\n\n")
    f.write(f"交易对: ${SYMBOL}\n")
    f.write(f"测试时长: ${DURATION}s\n\n")
    f.write("说明:\n")
    f.write("  - 测量延迟 = 真实延迟 + 时钟偏移（负值说明服务器时钟快）\n")
    f.write("  - 关注稳定性指标: 标准差、抖动（不受时钟偏移影响）\n")
    f.write("  - 真实延迟 = 测量中位数 - 估算时钟偏移\n\n")
    
    if ext:
        f.write("Extended Exchange\n" + "-" * 70 + "\n")
        f.write(f"消息数量:        {ext['n']}\n")
        f.write(f"频率:            {ext['n']/${DURATION}:.1f} msg/s\n\n")
        
        f.write("【测量延迟 (含时钟偏移)】\n")
        f.write(f"  平均:          {ext['mean']:>8.2f} ms\n")
        f.write(f"  中位数:        {ext['med']:>8.2f} ms\n")
        f.write(f"  范围:          {ext['min']:>8.2f} ~ {ext['max']:.2f} ms\n")
        f.write(f"  P5-P95:        {ext['p5']:>8.2f} ~ {ext['p95']:.2f} ms\n")
        f.write(f"  P99:           {ext['p99']:>8.2f} ms\n\n")
        
        f.write("【稳定性指标 (不受时钟偏移影响)】⭐\n")
        f.write(f"  标准差:        {ext['std']:>8.2f} ms  (越小越稳定)\n")
        f.write(f"  波动范围:      {ext['range']:>8.2f} ms  (max - min)\n")
        f.write(f"  平均抖动:      {ext['jitter_mean']:>8.2f} ms  (越小越好)\n")
        f.write(f"  P95抖动:       {ext['jitter_p95']:>8.2f} ms\n")
        f.write(f"  最大抖动:      {ext['jitter_max']:>8.2f} ms\n\n")
        
        f.write("【真实延迟估算】\n")
        f.write(f"  估算时钟偏移:  {ext['clock_offset']:>8.2f} ms\n")
        f.write(f"  估算真实延迟:  {ext['true_latency_estimate']:>8.2f} ms (中位数 - 偏移)\n\n")
    else:
        f.write("Extended: 无数据\n\n")
    
    if lgt:
        f.write("Lighter Network\n" + "-" * 70 + "\n")
        f.write(f"消息数量:        {lgt['n']}\n")
        f.write(f"频率:            {lgt['n']/${DURATION}:.1f} msg/s\n\n")
        
        f.write("【测量延迟 (含时钟偏移)】\n")
        f.write(f"  平均:          {lgt['mean']:>8.2f} ms\n")
        f.write(f"  中位数:        {lgt['med']:>8.2f} ms\n")
        f.write(f"  范围:          {lgt['min']:>8.2f} ~ {lgt['max']:.2f} ms\n")
        f.write(f"  P5-P95:        {lgt['p5']:>8.2f} ~ {lgt['p95']:.2f} ms\n")
        f.write(f"  P99:           {lgt['p99']:>8.2f} ms\n\n")
        
        f.write("【稳定性指标 (不受时钟偏移影响)】⭐\n")
        f.write(f"  标准差:        {lgt['std']:>8.2f} ms  (越小越稳定)\n")
        f.write(f"  波动范围:      {lgt['range']:>8.2f} ms  (max - min)\n")
        f.write(f"  平均抖动:      {lgt['jitter_mean']:>8.2f} ms  (越小越好)\n")
        f.write(f"  P95抖动:       {lgt['jitter_p95']:>8.2f} ms\n")
        f.write(f"  最大抖动:      {lgt['jitter_max']:>8.2f} ms\n\n")
        
        f.write("【真实延迟估算】\n")
        f.write(f"  估算时钟偏移:  {lgt['clock_offset']:>8.2f} ms\n")
        f.write(f"  估算真实延迟:  {lgt['true_latency_estimate']:>8.2f} ms (中位数 - 偏移)\n\n")
    else:
        f.write("Lighter: 无数据\n\n")
    
    if ext and lgt:
        f.write("=" * 70 + "\n")
        f.write("对比分析\n")
        f.write("=" * 70 + "\n\n")
        
        # 稳定性对比
        f.write("【稳定性对比】(关键指标)\n")
        more_stable_std = "Extended" if ext['std'] < lgt['std'] else "Lighter"
        more_stable_jitter = "Extended" if ext['jitter_mean'] < lgt['jitter_mean'] else "Lighter"
        more_stable_range = "Extended" if ext['range'] < lgt['range'] else "Lighter"
        
        f.write(f"  标准差更小:    {more_stable_std:>10s}  (Ext: {ext['std']:.2f} vs Lgt: {lgt['std']:.2f})\n")
        f.write(f"  抖动更小:      {more_stable_jitter:>10s}  (Ext: {ext['jitter_mean']:.2f} vs Lgt: {lgt['jitter_mean']:.2f})\n")
        f.write(f"  波动更小:      {more_stable_range:>10s}  (Ext: {ext['range']:.2f} vs Lgt: {lgt['range']:.2f})\n\n")
        
        # 频率对比
        ext_freq = ext['n'] / ${DURATION}
        lgt_freq = lgt['n'] / ${DURATION}
        more_freq = "Extended" if ext_freq > lgt_freq else "Lighter"
        f.write(f"【消息频率】\n")
        f.write(f"  更高频率:      {more_freq:>10s}  (Ext: {ext_freq:.1f} vs Lgt: {lgt_freq:.1f} msg/s)\n\n")
        
        # 真实延迟对比
        f.write(f"【估算真实延迟】\n")
        f.write(f"  Extended:      {ext['true_latency_estimate']:>8.2f} ms ± {ext['std']:.2f} (标准差)\n")
        f.write(f"  Lighter:       {lgt['true_latency_estimate']:>8.2f} ms ± {lgt['std']:.2f} (标准差)\n\n")
        
        # 综合评分
        ext_score = 0
        lgt_score = 0
        
        if ext['std'] < lgt['std']:
            ext_score += 2  # 标准差最重要，权重2
        else:
            lgt_score += 2
            
        if ext['jitter_mean'] < lgt['jitter_mean']:
            ext_score += 2  # 抖动也很重要，权重2
        else:
            lgt_score += 2
            
        if ext['range'] < lgt['range']:
            ext_score += 1
        else:
            lgt_score += 1
            
        if ext_freq > lgt_freq:
            ext_score += 1  # 频率加分
        else:
            lgt_score += 1
        
        winner = "Extended" if ext_score > lgt_score else "Lighter" if lgt_score > ext_score else "平局"
        
        f.write("=" * 70 + "\n")
        f.write(f"【综合评分】 {winner} 获胜\n")
        f.write("=" * 70 + "\n")
        f.write(f"  Extended:  {ext_score}/6 分\n")
        f.write(f"  Lighter:   {lgt_score}/6 分\n\n")
        
        f.write("评分标准:\n")
        f.write("  - 标准差更小: +2 分 (稳定性)\n")
        f.write("  - 抖动更小:   +2 分 (平滑度)\n")
        f.write("  - 波动更小:   +1 分 (一致性)\n")
        f.write("  - 频率更高:   +1 分 (实时性)\n\n")
        
        f.write("💡 建议:\n")
        if winner == "Extended":
            f.write("  - Extended 更稳定，推荐用于对冲套利\n")
            f.write("  - 标准差小，延迟可预测，适合精确定价\n")
        elif winner == "Lighter":
            f.write("  - Lighter 表现更好\n")
        else:
            f.write("  - 两者表现相当，可以都用\n")
    
    elif lgt and not ext:
        f.write("=" * 70 + "\n")
        f.write("注意: Extended 无数据，仅 Lighter 结果有效\n")

print(open("${REPORT}").read())
print(f"\n📁 数据文件:")
print(f"  Extended: ${EXTENDED_LOG}")
print(f"  Lighter:  ${LIGHTER_LOG}")
print(f"  Report:   ${REPORT}")
EOF

echo "✅ 完成"