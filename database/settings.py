from tortoise import Tortoise


async def initialize_database():
    await Tortoise.init(
        db_url="sqlite://database/db.sqlite3",
        modules={
            "models": [
                "database.models.tokens_data",
            ]
        },
        timezone="Europe/Moscow",
    )

    await Tortoise.generate_schemas()
