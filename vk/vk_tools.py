import json
import os
import redis
import requests
from dotenv import load_dotenv
import vk_api

def get_user_info(user_id, vk_token, fields="sex, bdate, city, country"):
    res = requests.get(
        'https://api.vk.com/method/users.get',
        params={
            'user_ids': user_id,
            'fields': fields,
            'access_token': vk_token,
            'v': 5.131,
        },
    ).json()

    user_info = res['response'][0]

    first_name = user_info['first_name']
    last_name = user_info['last_name']
    sex = user_info['sex']
    if sex == 1:
        sex = 'жен'
    elif sex == 2:
        sex = 'муж'
    else:
        sex = 'None'
    city = user_info.get('city', {}).get('title')
    country = user_info.get('country', {}).get('title')
    b_date = user_info.get('bdate', {})

    return first_name, last_name, b_date, sex, city, country

def upload_leaderboard(vk_token, group_id, r_conn):

    leaderboard_dict = r_conn.hgetall('accounts')
    sorted_leaderboard_dict = sorted(leaderboard_dict.items(), key=lambda x:x[1])
    leaderboard_file = json.dumps(leaderboard_dict)
    owner_id = f'-{group_id}'
    from_group = 1
    res = requests.get(
        'https://api.vk.com/method/wall.post',
        params={
            'owner_id': owner_id,
            'from_group': from_group,
            'message': 'Лидерборд',
            'access_token': vk_token,
            'v': 5.131,
        },
    ).json()
    # authorize.method('wall.post', {
    #     'owner_id': -int(group_id),
    #     'message': 'Лидерборд:'
    # })


def main():
    load_dotenv()
    user_id = os.getenv('user_id')
    vk_chat_token = os.getenv('vk_chat_token')
    vk_token = os.getenv('vk_token')
    host = os.getenv('host')
    port = os.getenv('port')

    first_name, last_name, sex, city, bdate, age = get_user_info(user_id, vk_token)
    print(first_name, last_name, sex, city, bdate, age)


    authorize = vk_api.VkApi(token=vk_token)
    # password = 0000
    db = 0

    r_conn = redis.Redis(
        host=host,
        port=port,
        db=db,
        charset='utf-8',
        decode_responses=True
    )
    upload_leaderboard(vk_token, user_id, r_conn)

if __name__ == "__main__":

    main()
