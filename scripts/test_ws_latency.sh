#!/bin/bash
# filepath: scripts/test_ws_latency.sh

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
    """
    标准化时间戳到毫秒
    ts: 服务器时间戳
    recv_time_ms: 本地接收时间（毫秒）
    """
    if ts == 0:
        return 0
    
    # 判断时间戳单位
    if ts > 1e15:  # 微秒 (16位以上)
        return ts / 1000
    elif ts > 1e12:  # 毫秒 (13-15位)
        return ts
    elif ts > 1e9:   # 秒 (10-12位)
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
                    recv_time = time.time() * 1000  # 毫秒
                    
                    data = json.loads(msg)
                    msg_type = data.get("type", "")
                    
                    if msg_type == "SNAPSHOT":
                        server_ts_raw = data.get("ts", 0)
                        
                        if server_ts_raw > 0:
                            server_ts = normalize_timestamp(server_ts_raw, recv_time)
                            latency = recv_time - server_ts
                            
                            # 首条消息打印调试信息
                            if first_msg:
                                print(f"   [调试] 服务器原始时间戳: {server_ts_raw}")
                                print(f"   [调试] 标准化后: {server_ts}")
                                print(f"   [调试] 本地时间: {recv_time}")
                                print(f"   [调试] 延迟: {latency:.1f}ms")
                                first_msg = False
                            
                            msg_count += 1
                            
                            with open(EXTENDED_LOG, 'a') as f:
                                f.write(f"{recv_time},{server_ts},{latency}\n")
                            
                            if msg_count % 50 == 0:
                                print(f"Extended: {msg_count} 条, 延迟 {latency:.1f}ms")
                    
                    elif msg_type == "PING":
                        await ws.send(json.dumps({"type": "PONG"}))
                
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"⚠️ Extended: {e}")
                    break
    
    except ConnectionResetError:
        print(f"❌ Extended 连接被重置")
    except asyncio.TimeoutError:
        print(f"❌ Extended 连接超时")
    except Exception as e:
        print(f"❌ Extended 失败: {type(e).__name__}: {e}")
    
    if msg_count > 0:
        print(f"✅ Extended: {msg_count} 条")
    else:
        print(f"⚠️ Extended: 无数据")

async def test_lighter():
    import requests
    
    try:
        resp = requests.get("https://mainnet.zklighter.elliot.ai/api/v1/orderBooks", timeout=10)
        data = resp.json()
        
        market_id = None
        for m in data.get("order_books", []):
            symbol = m["symbol"]
            if symbol == SYMBOL:
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
                                print(f"   [调试] 服务器原始时间戳: {server_ts_raw}")
                                print(f"   [调试] 标准化后: {server_ts}")
                                print(f"   [调试] 本地时间: {recv_time}")
                                print(f"   [调试] 延迟: {latency:.1f}ms")
                                first_msg = False
                            
                            msg_count += 1
                            
                            with open(LIGHTER_LOG, 'a') as f:
                                f.write(f"{recv_time},{server_ts},{latency}\n")
                            
                            if msg_count % 50 == 0:
                                print(f"Lighter: {msg_count} 条, 延迟 {latency:.1f}ms")
                    
                    elif msg_type == "ping":
                        await ws.send(json.dumps({"type": "pong"}))
                
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"⚠️ Lighter: {e}")
                    break
    
    except Exception as e:
        print(f"❌ Lighter 失败: {e}")
    
    print(f"✅ Lighter: {msg_count} 条")

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
    return {
        'n': len(lat),
        'mean': statistics.mean(lat),
        'med': statistics.median(lat),
        'min': min(lat),
        'max': max(lat),
        'std': statistics.stdev(lat) if len(lat) > 1 else 0,
        'p95': s[int(len(s)*0.95)],
        'p99': s[int(len(s)*0.99)]
    }

ext = analyze("${EXTENDED_LOG}")
lgt = analyze("${LIGHTER_LOG}")

with open("${REPORT}", 'w') as f:
    f.write("=" * 60 + "\n")
    f.write("WebSocket 延迟测试\n")
    f.write("=" * 60 + "\n\n")
    f.write(f"交易对: ${SYMBOL}\n")
    f.write(f"时长: ${DURATION}s\n\n")
    
    if ext:
        f.write("Extended\n" + "-" * 60 + "\n")
        f.write(f"消息: {ext['n']}\n")
        f.write(f"平均: {ext['mean']:.2f} ms\n")
        f.write(f"中位: {ext['med']:.2f} ms\n")
        f.write(f"P95: {ext['p95']:.2f} ms\n")
        f.write(f"P99: {ext['p99']:.2f} ms\n")
        f.write(f"范围: {ext['min']:.2f} - {ext['max']:.2f}\n\n")
    else:
        f.write("Extended: 无数据\n\n")
    
    if lgt:
        f.write("Lighter\n" + "-" * 60 + "\n")
        f.write(f"消息: {lgt['n']}\n")
        f.write(f"平均: {lgt['mean']:.2f} ms\n")
        f.write(f"中位: {lgt['med']:.2f} ms\n")
        f.write(f"P95: {lgt['p95']:.2f} ms\n")
        f.write(f"P99: {lgt['p99']:.2f} ms\n")
        f.write(f"范围: {lgt['min']:.2f} - {lgt['max']:.2f}\n\n")
    else:
        f.write("Lighter: 无数据\n\n")
    
    if ext and lgt:
        f.write("=" * 60 + "\n对比\n" + "=" * 60 + "\n")
        faster = "Extended" if ext['mean'] < lgt['mean'] else "Lighter"
        diff = abs(ext['mean'] - lgt['mean'])
        f.write(f"更快: {faster}\n")
        f.write(f"差值: {diff:.2f}ms\n")
    elif lgt and not ext:
        f.write("=" * 60 + "\n")
        f.write("注意: Extended 无数据，仅 Lighter 结果有效\n")

print(open("${REPORT}").read())
print(f"\n数据文件:")
print(f"  Extended: ${EXTENDED_LOG}")
print(f"  Lighter:  ${LIGHTER_LOG}")
EOF

echo "✅ 完成"