import asyncio
import random

import httpx
from loguru import logger

from database import TokensData
from main import CryptoParser
from utils import split_into_groups


class PriceChange(CryptoParser):
    def __init__(self):
        super().__init__()
        self.symbols_price_change_per_5m_data: dict = {}

    async def __fetch_price_request(self, url: str, proxy: str, symbol: str) -> None:
        async with httpx.AsyncClient(proxies=proxy) as session:
            try:
                response = await session.get(url, timeout=3)
                response.raise_for_status()
                price_per_5m = response.json()["pairs"][0]["priceChange"]["m5"]
                self.symbols_price_change_per_5m_data[symbol] = float(price_per_5m)

            except Exception as error:
                logger.debug(f"Price request failed | Details: {error}")
                self.symbols_price_change_per_5m_data[symbol] = 0

    async def get_price_change_value_every_5m(self):
        tokens_contracts = await TokensData().get_tokens_contracts()

        while True:
            for contracts_group in split_into_groups(tokens_contracts, 20):
                if not self.proxies:
                    await self.load_proxy()

                proxy_str = self.proxies[0]
                self.proxies.pop(0)

                tasks = []
                for contract in contracts_group:
                    # contract[0] is a token_symbol
                    # contract[1] is a dict with contracts

                    if not list(contract[1].values()):
                        continue

                    else:
                        tasks.append(
                            asyncio.create_task(
                                self.__fetch_price_request(
                                    f"https://api.dexscreener.com/latest/dex/tokens/{random.choice(list(contract[1].values()))}",
                                    proxy_str,
                                    contract[0],
                                )
                            )
                        )

                await asyncio.gather(*tasks)

            # Update orders in database
            tasks = [
                asyncio.create_task(
                    TokensData().update_token_price_change_per_5m(price, token_symbol)
                )
                for token_symbol, price in self.symbols_price_change_per_5m_data.items()
            ]
            await asyncio.gather(*tasks)
            logger.debug("Price updated")
            await asyncio.sleep(300)
