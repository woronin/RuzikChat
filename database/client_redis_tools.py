import re
import json

def scale_text(answer):
    return re.sub(r'[\(\[].*?[\)\]]', "", answer).strip()
def upload_last_question(user_id, question, r_conn):
    r_conn.hset(
        'last_question',
        f'id_{user_id}',
        question
    )

def get_last_question(user_id, r_conn):
    question = r_conn.hget('last_question', f'id_{user_id}')

    return question

def upload_last_correct_answer(user_id, answer, r_conn):
    r_conn.hset(
        'last_correct_answer',
        f'id_{user_id}',
        answer
    )
def get_last_correct_answer(user_id, r_conn):
    correct_answer = r_conn.hget('last_correct_answer', f'id_{user_id}')

    return correct_answer

def get_users_info(r_conn):
    return r_conn.hgetall('users_info')

def get_user_qa(user_id, r_conn):
    return r_conn.hgetall(f'questions_{user_id}'), r_conn.hgetall(f'correct_answers_{user_id}')
def upload_account(bonus, user_id, r_conn):
    user_info_dict = json.loads(
        r_conn.hget('users_info', f'id_{user_id}')
    )

    if r_conn.hget('users_info', f'id_{user_id}') is not None:
        total_account = int(user_info_dict['account'])
        total_account += bonus
    else:
        total_account = bonus

    user_info_dict['account'] = total_account
    r_conn.hset('users_info', f'id_{user_id}', json.dumps(user_info_dict))

def upload_user_info(user_id, bdate, city, country, sex, bonus, r_conn):
    if not r_conn.hgetall('users_info') or r_conn.hget('users_info', f'id_{user_id}') is None:
        r_conn.hset('users_info', f'id_{user_id}', )

def clear_account(user_id, r_conn):
    r_conn.hdel('accounts', f'id_{user_id}')

def clear_all_account(r_conn):
    r_conn.delete('accounts')

def clear_answer(user_id, r_conn):
    r_conn.hdel('answers', f'id_{user_id}')

def clear_all_answer(r_conn):
    r_conn.delete('answers')
