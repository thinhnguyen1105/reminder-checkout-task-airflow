import requests
import config


def sendMessage(message):
    telegram_api_url = f"https://api.telegram.org/bot{config.telegram_token}/sendMessage?chat_id={config.telegram_group_id}&text={message}"
    tel_resp = requests.get(telegram_api_url)
    if tel_resp.status_code == 200:
        print(f'Sent message to {config.telegram_group_id}')
    else:
        print('Send messsage fail')
