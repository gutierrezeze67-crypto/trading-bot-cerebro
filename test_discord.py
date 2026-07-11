import discord
from config.settings import DISCORD_BOT_TOKEN

intents = discord.Intents.all()

class TestBot(discord.Client):
    async def on_ready(self):
        print(f"✅ Conectado como: {self.user.name}")
        print(f"📡 Escuchando TODOS los canales...")
    
    async def on_message(self, msg):
        if msg.author == self.user:
            return
        print(f"\n📨 MENSAJE DETECTADO")
        print(f"   Canal ID: {msg.channel.id}")
        print(f"   Canal: #{msg.channel.name}")
        print(f"   Autor: {msg.author.name}")
        print(f"   Contenido: {msg.content[:100]}")
        print(f"   Attachments: {len(msg.attachments)}")
        for att in msg.attachments:
            print(f"   🖼️ {att.filename} | {att.content_type} | {att.url}")

bot = TestBot(intents=intents)
print("🚀 Iniciando diagnóstico...")
bot.run(DISCORD_BOT_TOKEN)
