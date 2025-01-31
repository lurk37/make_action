# 모듈을 읽어 들입니다.
import requests

# 요청을 위한 상수를 선언합니다: TOKEN은 자신의 것을 입력해주세요.
TARGET_URL = 'https://notify-api.line.me/api/notify'
TOKEN = 'O29g8uksLk1dI8nc1bCsbk0pRTCngbKumToh5RFxbtu'

def SendMessage(msg):
    try:
        response = requests.post(
            TARGET_URL,
            headers={
                'Authorization': 'Bearer ' + TOKEN
            },
            data={
            'message': msg
            }
        )
    except Exception as ex:
        print(ex)