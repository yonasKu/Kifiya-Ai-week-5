from telethon import TelegramClient, types
import csv
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv('.env')
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')

# Function to fetch messages from a single channel and write to CSV
async def fetch_messages(client, channel_link, writer, media_dir, limit=100):
    entity = await client.get_entity(channel_link)
    channel_title = entity.title  # Get the channel's title

    async for message in client.iter_messages(entity, limit=limit):
        media_path = None
        if message.media:
            # Handle different media types
            if isinstance(message.media, types.MessageMediaPhoto):
                filename = f"photo_{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                await client.download_media(message.media, media_path)
            elif isinstance(message.media, types.MessageMediaDocument):
                filename = f"doc_{message.id}"
                media_path = os.path.join(media_dir, filename)
                await client.download_media(message.media, media_path)
            elif isinstance(message.media, types.MessageMediaWebPage):
                media_path = f"Web page URL: {message.media.webpage.url}" if message.media.webpage else "Unknown webpage"
        
        # Write message details to CSV
        writer.writerow([
            channel_title,
            channel_link,
            message.sender_id,
            message.date.isoformat(),
            message.message if message.message else '',
            media_path if media_path else ''
        ])

# Initialize the Telegram client
client = TelegramClient('scraper_session', api_id, api_hash, timeout=60)

async def main():
    await client.start()

    # Create a directory for media files
    media_dir = 'media'
    os.makedirs(media_dir, exist_ok=True)

    # Open the CSV file and prepare the writer
    with open('telegram_messages.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # CSV headers including channel details, sender, message, etc.
        writer.writerow(['Channel Title', 'Channel Link', 'Sender ID', 'Timestamp', 'Message', 'Media Path'])
        
        # List of channels to scrape
        channels = [
            'https://t.me/ZemenExpress',  # Scrape from the ZemenExpress channel
        ]
        
        # Fetch messages from each channel and write to the CSV
        for channel in channels:
            await fetch_messages(client, channel, writer, media_dir, limit=4000)
            print(f"Finished scraping data from {channel}")

# Run the main function
with client:
    client.loop.run_until_complete(main())
