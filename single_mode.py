#extended 市价单秒开秒关的入口
import argparse
import asyncio
from decimal import Decimal
from pathlib import Path
import dotenv
# python single_mode.py --exchange extended --ticker BTC --size 0.0001 --iter 2 --side buy --env-file .env.extend.main1 
def parse_arguments():
    parser = argparse.ArgumentParser(description='Single Mode Trading Bot')
    parser.add_argument('--exchange', type=str, required=True, help='Exchange to use')
    parser.add_argument('--ticker', type=str, default='BTC')
    parser.add_argument('--size', type=str, required=True)
    parser.add_argument('--iter', type=int, required=True)
    parser.add_argument('--side', type=str, required=True, choices=['buy', 'sell'], help='Order side')
    parser.add_argument('--interval', type=int, default=6, help='开平仓间隔，秒')
    parser.add_argument('--env-file', type=str, default=".env")
    return parser.parse_args()

def get_single_bot_class(exchange):
    if exchange.lower() == 'extended':
        from single_mode.single_mode_ext import SingleBot
        return SingleBot
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")

async def main():
    args = parse_arguments()
    dotenv.load_dotenv(args.env_file)
    SingleBotClass = get_single_bot_class(args.exchange)
    bot = SingleBotClass(
        ticker=args.ticker.upper(),
        order_quantity=Decimal(args.size),
        iterations=args.iter,
        interval=args.interval,
        side=args.side
    )
    
    try:
        await bot.run()
    finally:
        # bot.run() 里的 finally 已经调用了 shutdown()，这里不需要重复调用
        
        # 等待所有异步任务完成
        await asyncio.sleep(1)
        
        # 取消所有未完成的任务
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            for task in tasks:
                task.cancel()
            
            # 等待任务取消完成
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # 最后再等待一下，确保所有资源释放
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(main())