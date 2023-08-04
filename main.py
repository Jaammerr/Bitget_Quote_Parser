import asyncio
import httpx
import aiofiles
import numpy as np

from loguru import logger
from database import TokensData


class CryptoParser:
    def __init__(self):
        self.session = httpx.AsyncClient(timeout=5)
        self.spot_tokens_symbols: list = []

        # Bitget has custom token name for getting data, for example: 'trx' -> 'TRXUSDT_SPBL'
        self.spot_tokens_symbols_for_bitget: list = []
        self.spot_tokens_contracts: dict = {}
        self.get_token_original_symbol: dict = {}

        # Coingecko has custom token ids, for example: 'eth' -> 'ethereum'
        self.coingecko_tokens: dict = {}
        self.proxies: list[str] = []

    async def load_proxy(self) -> None:
        async with aiofiles.open("proxy.txt", "r") as fp:
            proxies = await fp.readlines()

        for proxy in proxies:
            proxy = proxy.strip().split(":")
            ip, port, user, password = proxy
            proxy = f"http://{user}:{password}@{ip}:{port}"
            self.proxies.append(proxy)

        logger.debug(f"{len(proxies)} proxies loaded")

    async def get_coingecko_tokens_list(self) -> None:
        response = await self.session.get("https://api.coingecko.com/api/v3/coins/list")
        self.coingecko_tokens = {
            token.get("symbol"): token.get("id") for token in response.json()
        }
        logger.debug("Coingecko tokens list loaded")

    async def get_spot_tokens_symbols(self) -> None:
        response = await self.session.get(
            "https://api.bitget.com/api/spot/v1/public/products"
        )
        self.spot_tokens_symbols = [
            token.get("baseCoin").lower() for token in response.json().get("data")
        ]
        self.spot_tokens_symbols_for_bitget = [
            token.get("symbol") for token in response.json().get("data")
        ]
        self.get_token_original_symbol = {
            token.get("symbol"): token.get("baseCoin").lower()
            for token in response.json().get("data")
        }

        # Add tokens symbols to database
        tasks = [
            asyncio.create_task(TokensData().add_token_symbol(token_symbol))
            for token_symbol in self.spot_tokens_symbols
        ]
        await asyncio.gather(*tasks)
        logger.debug("Tokens symbols added")

    async def __fetch_contract_request(self, url: str, proxy: str, symbol: str):
        while True:
            async with httpx.AsyncClient(proxies=proxy) as session:
                try:
                    response = await session.get(url, timeout=3)
                    response.raise_for_status()
                    platforms = response.json().get("platforms")
                    contract_info = {}

                    if "ethereum" in platforms:
                        contract_info["eth"] = platforms.get("ethereum")

                    if "binance-smart-chain" in platforms:
                        contract_info["bsc"] = platforms.get("binance-smart-chain")

                    self.spot_tokens_contracts[symbol] = (
                        contract_info if contract_info else {}
                    )
                    return

                except Exception as error:
                    logger.debug(f"Contract request failed | Details: {error}")
                    await asyncio.sleep(3)

    async def get_spot_tokens_contracts(self) -> None:
        logger.debug("Updating contracts..")

        await self.load_proxy()
        symbols_chunks = np.array_split(self.spot_tokens_symbols, len(self.proxies))

        for symbols, proxy in zip(symbols_chunks, self.proxies):
            tasks = []

            for symbol in symbols:
                coingecko_token_id = self.coingecko_tokens.get(str(symbol))
                if coingecko_token_id:
                    tasks.append(
                        asyncio.create_task(
                            self.__fetch_contract_request(
                                f"https://api.coingecko.com/api/v3/coins/{coingecko_token_id}",
                                proxy,
                                symbol,
                            )
                        )
                    )

            await asyncio.gather(*tasks)

        # Update contracts in database
        tasks = [
            asyncio.create_task(
                TokensData().update_token_contracts(contracts, token_symbol)
            )
            for token_symbol, contracts in self.spot_tokens_contracts.items()
        ]
        await asyncio.gather(*tasks)
        logger.debug("Contracts updated")
