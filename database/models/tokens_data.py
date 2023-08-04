from tortoise import Model, fields
from tortoise.exceptions import IntegrityError
from tortoise.expressions import Q


class TokensData(Model):
    token_symbol = fields.TextField(pk=True)
    token_orders = fields.JSONField(default={})
    token_contracts = fields.JSONField(default={})
    token_price_change_per_5m = fields.TextField(default="0")

    async def exists_token(self, token_symbol: str) -> bool:
        return await self.filter(token_symbol=token_symbol).exists()

    async def add_token_symbol(self, token_symbol: str) -> None:
        if not await self.exists_token(token_symbol):
            try:
                await self.create(token_symbol=token_symbol)
            except IntegrityError:
                return

    async def update_token_contracts(self, contracts: dict, token_symbol: str) -> None:
        if contracts:
            try:
                await self.filter(token_symbol=token_symbol).update(
                    token_contracts=contracts
                )
            except IntegrityError:
                return

    async def update_token_orders(self, orders: list, token_symbol: str) -> None:
        try:
            await self.filter(token_symbol=token_symbol).update(token_orders=orders)
        except IntegrityError:
            return

    async def update_token_price_change_per_5m(
        self, price_change_per_5m: int, token_symbol: str
    ) -> None:
        try:
            await self.filter(token_symbol=token_symbol).update(
                token_price_change_per_5m=f"{price_change_per_5m}%"
            )
        except IntegrityError:
            return

    async def get_tokens_contracts(self) -> list[tuple[str, dict]]:
        return (
            await self.filter(~Q(token_contracts__isnull=True))
            .all()
            .values_list("token_symbol", "token_contracts")
        )
