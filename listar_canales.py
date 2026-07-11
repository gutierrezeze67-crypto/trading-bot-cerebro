import discord
import asyncio
from config.settings import DISCORD_BOT_TOKEN

intents = discord.Intents.default()
intents.guilds = True

class MiBot(discord.Client):
    async def on_ready(self):
        print(f"\n🤖 Conectado como: {self.user.name}")
        print(f"📋 Canales visibles:\n")
        for guild in self.guilds:
            print(f"Servidor: {guild.name}")
            for channel in guild.channels:
                if channel.type.name == 'text':
                    print(f"  ID: {channel.id} | #{channel.name}")
        print("\n✅ Listo. Copia el ID del canal de Trading Different.")
        await self.close()

bot = MiBot(intents=intents)
bot.run(DISCORD_BOT_TOKEN)
