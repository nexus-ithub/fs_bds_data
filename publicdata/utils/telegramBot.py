import telegram as tel
import asyncio


class TelegramBot:
    def __init__(self, token: str, chat_id: int):
        self.bot = tel.Bot(token=token)
        self.chat_id = chat_id

    async def send_message(self, text):
        # return 
        await self.bot.send_message(chat_id=self.chat_id, text=text)
