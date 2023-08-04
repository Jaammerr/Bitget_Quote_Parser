import asyncio

import httpx
from loguru import logger

from database import TokensData
from main import CryptoParser
from utils import split_into_groups


class Orders(CryptoParser):
    def __init__(self):
        super().__init__()
        self.symbols_orders_data: dict = {}

    @staticmethod
    async def __fetch_order_request(url: str, proxy: str):
        async with httpx.AsyncClient(proxies=proxy) as session:
            try:
                response = await session.get(url, timeout=3)
                response.raise_for_status()
                return response.json()

            except Exception as error:
                logger.debug(f"Order request failed | Details: {error}")
                return None

    async def __get_responses_data(self, token_symbols_group, responses) -> None:
        for token_symbol, response in zip(token_symbols_group, responses):
            token_symbol = self.get_token_original_symbol.get(token_symbol)

            if response is not None and int(response.get("code")) == 00000:
                orders_data = response.get("data")["asks"]
                self.symbols_orders_data[token_symbol] = orders_data

            else:
                self.symbols_orders_data[token_symbol] = None

    async def get_spot_tokens_orders_every_1m(self) -> None:
        await self.get_spot_tokens_symbols()

        while True:
            for token_symbols_group in split_into_groups(
                self.spot_tokens_symbols_for_bitget, 20
            ):
                if not self.proxies:
                    await self.load_proxy()

                proxy_str = self.proxies[0]
                self.proxies.pop(0)

                tasks = [
                    asyncio.create_task(
                        self.__fetch_order_request(
                            f"https://api.bitget.com/api/spot/v1/market/depth?symbol={token_symbol}&type=step0&limit=100",
                            proxy_str,
                        )
                    )
                    for token_symbol in token_symbols_group
                ]

                responses = await asyncio.gather(*tasks)
                await self.__get_responses_data(token_symbols_group, responses)

            # Update orders in database
            tasks = [
                asyncio.create_task(
                    TokensData().update_token_orders(orders, token_symbol)
                )
                for token_symbol, orders in self.symbols_orders_data.items()
            ]
            await asyncio.gather(*tasks)
            logger.debug("Orders updated")
            await asyncio.sleep(60)
