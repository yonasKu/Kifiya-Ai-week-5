# import os
# from dotenv import load_dotenv
# from telethon import TelegramClient, events
# import pandas as pd
# import json
# import nltk
# from nltk.tokenize import word_tokenize
# import re

# # Load environment variables from .env file
# load_dotenv()

# # Fetch API ID and API Hash from environment variables
# api_id = os.getenv('TELEGRAM_API_ID')
# api_hash = os.getenv('TELEGRAM_API_HASH')

# # Initialize the Telegram client
# client = TelegramClient('scraper_session', api_id, api_hash)

# # Set the Ethiopian-based e-commerce Telegram channel to scrape
# channel = 'https://t.me/ZemenExpress'  # or use '@ZemenExpress'

# # List to store processed messages
# processed_data = []

# # Function for preprocessing Amharic text
# def preprocess_text(text):
#     # Normalize text (remove extra spaces, lower case, etc.)
#     text = re.sub(r'\s+', ' ', text).strip().lower()
    
#     # Tokenize the text
#     tokens = word_tokenize(text)
    
#     # Further normalization can be added here if needed
#     return tokens

# # Event listener for new messages
# @client.on(events.NewMessage(chats=channel))
# async def handler(event):
#     message = event.message
    
#     # Extract metadata and content
#     sender_id = message.sender_id
#     timestamp = message.date.isoformat()
#     content = message.message
#     media_type = None
#     media_id = None

#     # Handle media types
#     if message.media:
#         if hasattr(message.media, 'document'):
#             media_type = 'document'
#             media_id = message.media.document.id
#         elif hasattr(message.media, 'photo'):
#             media_type = 'photo'
#             media_id = message.media.photo.id
#         elif hasattr(message.media, 'video'):
#             media_type = 'video'
#             media_id = message.media.video.id

#     # Preprocess text content
#     preprocessed_content = preprocess_text(content) if content else []

#     # Structure the message data
#     message_data = {
#         "sender_id": sender_id,
#         "timestamp": timestamp,
#         "content": preprocessed_content,
#         "media_type": media_type,
#         "media_id": media_id
#     }
    
#     processed_data.append(message_data)

#     # Save data to JSON file (you could also save to CSV or a database)
#     file_name = 'processed_messages.json'
#     with open(file_name, 'a', encoding='utf-8') as f:
#         json.dump(message_data, f, ensure_ascii=False)
#         f.write('\n')  # Append newline for each entry

#     print(f"Processed new message from {sender_id} at {timestamp}")

# # Start the client
# async def main():
#     async with client:
#         print("Listening for new messages from @ZemenExpress...")
#         await client.run_until_disconnected()

# # Run the script
# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())

import os
import json
import re
import time
from dotenv import load_dotenv
from telethon import TelegramClient, events
import nltk
from nltk.tokenize import word_tokenize
import asyncio

# Ensure NLTK data is downloaded for tokenization
nltk.download('punkt')

# Load environment variables from .env file
load_dotenv()

# Fetch API ID and API Hash from environment variables
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')

class TelegramScraper:
    def __init__(self, channel):
        self.channel = channel
        self.client = TelegramClient('scraper_session', api_id, api_hash)
        self.processed_data = []

        # Define where you want to store processed data
        self.data_path = os.path.join(os.path.dirname(__file__), '..', 'notebooks', 'processed_messages.json')

    async def message_handler(self, event):
        """Handle incoming messages and extract relevant data."""
        message = event.message

        # Extract metadata and content
        sender_id = message.sender_id
        timestamp = message.date.isoformat()
        content = message.message or ""
        media_type = None
        media_id = None
        media_path = None

        # Handle media types (image, documents, etc.)
        if message.media:
            media_path = await self.download_media(message)
            if hasattr(message.media, 'document'):
                media_type = 'document'
                media_id = message.media.document.id
            elif hasattr(message.media, 'photo'):
                media_type = 'photo'
                media_id = message.media.photo.id
            elif hasattr(message.media, 'video'):
                media_type = 'video'
                media_id = message.media.video.id

        # Preprocess text content
        preprocessed_content = self.preprocess_text(content)

        # Structure the message data
        message_data = {
            "metadata": {
                "sender_id": sender_id,
                "timestamp": timestamp,
                "media_type": media_type,
                "media_id": media_id,
                "media_path": media_path
            },
            "content": preprocessed_content
        }

        self.processed_data.append(message_data)

        # Save data to JSON file at the correct path
        with open(self.data_path, 'a', encoding='utf-8') as f:
            json.dump(message_data, f, ensure_ascii=False)
            f.write('\n')

        print(f"Processed new message from {sender_id} at {timestamp}")

    async def download_media(self, message):
        """Download media from the message and return the file path."""
        media_path = None
        if message.media:
            media_path = await self.client.download_media(message, 'media/')
        return media_path

    async def start_scraping_with_timeout(self, timeout_seconds=600):
        """Start listening for new messages with a timeout mechanism."""
        start_time = time.time()

        @self.client.on(events.NewMessage(chats=self.channel))
        async def handler(event):
            await self.message_handler(event)

        async with self.client:
            print("Listening for new messages...")
            while time.time() - start_time < timeout_seconds:
                await asyncio.sleep(1)  # Keep the loop alive for the timeout duration
            
            self.stop_scraping = True  # Set flag to stop scraping
            print("Stopping the listener after timeout.")
            await self.client.disconnect()  # Disconnect the client

# Function to run the scraper
async def run_scraper(channel, timeout=600):
    scraper = TelegramScraper(channel)
    await scraper.start_scraping_with_timeout(timeout)
