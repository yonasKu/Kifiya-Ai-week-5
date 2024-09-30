from telethon import TelegramClient, types
import csv
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv('.env')
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('phone')

# Function to sanitize file names
def sanitize_filename(filename):
    return "".join(c for c in filename if c.isalnum() or c in (' ', '_')).rstrip()

# Function to scrape data from a single channel
async def scrape_channel(client, channel_username, writer, media_dir):
    try:
        entity = await client.get_entity(channel_username)
        channel_title = entity.title  # Extract the channel's title
        async for message in client.iter_messages(entity, limit=4000):
            media_path = None

            if message.media:
                if hasattr(message.media, 'photo'):
                    # Handle photo media
                    filename = sanitize_filename(f"{channel_username}_{message.id}.jpg")
                    media_path = os.path.join(media_dir, filename)
                    await client.download_media(message.media, media_path)

                elif hasattr(message.media, 'document'):
                    # Handle document media
                    doc_attributes = message.media.document.attributes
                    if doc_attributes:
                        # Check for filename in the document attributes
                        for attr in doc_attributes:
                            if isinstance(attr, types.DocumentAttributeFilename):
                                filename = sanitize_filename(f"{channel_username}_{message.id}_{attr.file_name}")
                                media_path = os.path.join(media_dir, filename)
                                await client.download_media(message.media, media_path)
                                break
                        else:
                            # If no filename found, use a default name
                            filename = sanitize_filename(f"{channel_username}_{message.id}.bin")
                            media_path = os.path.join(media_dir, filename)
                            await client.download_media(message.media, media_path)

                # You can add more media types like video, audio, etc. here as needed

            # Write the channel title along with other data to CSV
            writer.writerow([
                channel_title, channel_username, message.id, 
                message.message or '',  # Ensure message is not None
                message.date.isoformat(),  # Format the date
                media_path or ''  # Handle case where no media is present
            ])

    except Exception as e:
        print(f"Error occurred while scraping {channel_username}: {str(e)}")

# Initialize the Telegram client
client = TelegramClient('scraping_session', api_id, api_hash)

async def main():
    await client.start()

    # Create a directory for media files
    media_dir = 'photos'
    os.makedirs(media_dir, exist_ok=True)

    # Open the CSV file and prepare the writer
    with open('telegram_data.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # CSV headers including channel title
        writer.writerow(['Channel Title', 'Channel Username', 'ID', 'Message', 'Date', 'Media Path'])
        
        # List of channels to scrape
        channels = [
            'https://t.me/ZemenExpress',  # Example channel
            # Add more channels as needed
        ]
        
        # Iterate over channels and scrape data
        for channel in channels:
            print(f"Scraping data from {channel}...")
            await scrape_channel(client, channel, writer, media_dir)
            print(f"Finished scraping data from {channel}")

with client:
    client.loop.run_until_complete(main())
