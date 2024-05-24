# # root/data_collection/scraping/scripts/telegram_spider.py

# import json
# from telethon.sync import TelegramClient
# from telethon.tl.functions.messages import GetHistoryRequest
# from telethon.tl.types import PeerChannel
# import datetime

# # Replace with your own values from my.telegram.org
# api_id = 'your_api_id'
# api_hash = 'your_api_hash'
# phone = 'your_phone_number'
# channel_username = 'your_channel_username'  # e.g., 'example_channel'

# client = TelegramClient(phone, api_id, api_hash)

# async def main():
#     await client.start()
#     print("Client Created")

#     # Ensure you're authorized
#     if not await client.is_user_authorized():
#         await client.send_code_request(phone)
#         await client.sign_in(phone, input('Enter the code: '))

#     try:
#         entity = await client.get_entity(channel_username)
#         channel = PeerChannel(entity.id)

#         offset_id = 0
#         limit = 100  # Number of messages to retrieve per request
#         all_messages = []

#         while True:
#             history = await client(GetHistoryRequest(
#                 peer=channel,
#                 offset_id=offset_id,
#                 offset_date=None,
#                 add_offset=0,
#                 limit=limit,
#                 max_id=0,
#                 min_id=0,
#                 hash=0
#             ))
            
#             if not history.messages:
#                 break
            
#             messages = history.messages
#             for message in messages:
#                 all_messages.append({
#                     'id': message.id,
#                     'date': message.date.isoformat(),
#                     'message': message.message,
#                     'from_id': message.from_id,
#                     'channel_id': entity.id,
#                 })
            
#             offset_id = messages[-1].id
        
#         with open('../../raw_data/telegram_data.json', 'w', encoding='utf-8') as f:
#             json.dump(all_messages, f, ensure_ascii=False, indent=4)
        
#         print(f"Saved {len(all_messages)} messages to telegram_data.json")
    
#     except Exception as e:
#         print(f"An error occurred: {e}")

# with client:
#     client.loop.run_until_complete(main())
