import asyncio

from loguru import logger

from database import initialize_database
from main import CryptoParser
from orders import Orders
from price_change import PriceChange


async def run():
    try:
        parser = CryptoParser()
        await parser.get_coingecko_tokens_list()
        await parser.get_spot_tokens_symbols()
        await parser.get_spot_tokens_contracts()

        price_change = PriceChange()
        orders = Orders()

        tasks = [
            asyncio.create_task(price_change.get_price_change_value_every_5m()),
            asyncio.create_task(orders.get_spot_tokens_orders_every_1m()),
        ]
        await asyncio.gather(*tasks)

    except Exception as error:
        logger.error(
            f"Uncaught exception while running script | Details: {error} | Trying again.."
        )
        await run()


if __name__ == "__main__":
    asyncio.run(initialize_database())
    asyncio.run(run())
