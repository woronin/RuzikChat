import os
import redis
from dotenv import load_dotenv

def get_last_num_of_questions(r_conn, user_id):
    if not r_conn.hgetall('last_num_of_questions') or r_conn.hget('last_num_of_questions', user_id) is None:
        return 0
    return r_conn.hget('last_num_of_questions', user_id)

def set_last_num_of_questions(r_conn, user_id, num):
    r_conn.hset('last_num_of_questions', user_id, num)
def upload_all_files_of_qa(r_conn, directory_name='questions_data', user_id=0):
    path_directory = os.path.abspath(f'./{directory_name}')
    path_directory = path_directory.replace('\\', '/')
    h_name = f'questions_{user_id}'
    question_counter = r_conn.hlen(h_name)
    for file_name in os.listdir(path_directory):
        # "KOI8-R"
        with open(f'{path_directory}/{file_name}', 'r', encoding='utf-8') as file:
            file = file.read().split("\n\n")
            for text in file:
                if text.find('Вопрос:') != -1 and text.find('Ответ:')!= -1:
                    question = text.splitlines()[0]
                    start_index = question.strip().find('Вопрос:')
                    scaled_question = ''.join(question[start_index+8:])

                    answer = text.splitlines()[1]
                    start_index = answer.find('Ответ:')
                    scaled_answer = ''.join(answer[start_index+7:])

                    r_conn.hset(f'questions_id_{user_id}', f'{question_counter}', scaled_question)
                    r_conn.hset(f'correct_answers_id_{user_id}', f'{question_counter}', scaled_answer)

                    question_counter += 1

def upload_one_file_of_qa(users_id, r_conn, file_name):
    directory_name = 'questions_data'
    path_directory = os.path.abspath(f'./{directory_name}')
    path_directory = path_directory.replace('\\', '/')

    for user_id in users_id:
        h_name = f'questions_{user_id}'
        question_counter = int(get_last_num_of_questions(r_conn, user_id))

        with open(f'{path_directory}/{file_name}', 'r', encoding='utf-8') as file:
            file = file.read().split("\n\n")
            for text in file:
                if text.find('Вопрос:') != -1 and text.find('Ответ:') != -1:
                    question = text.splitlines()[0]
                    start_index = question.strip().find('Вопрос:')
                    scaled_question = ''.join(question[start_index+8:])

                    answer = text.splitlines()[1]
                    start_index = answer.find('Ответ:')
                    scaled_answer = ''.join(answer[start_index+7:])

                    # print(scaled_question)
                    # print(scaled_answer)

                    r_conn.hset(f'questions_{user_id}', f'{question_counter}', scaled_question)
                    r_conn.hset(f'correct_answers_{user_id}', f'{question_counter}', scaled_answer)
                    question_counter += 1
        set_last_num_of_questions(r_conn, user_id, question_counter)

def change_admin_login(new_login, r_conn):
    r_conn.hset('admin', 'login', new_login)

def change_admin_password(new_password, r_conn):
    r_conn.hset('admin', 'password', new_password)

def delete_all_questions(user_id, r_conn):
    r_conn.delete(f'questions_{user_id}')

def delete_all_correct_answers(user_id, r_conn):
    r_conn.delete(f'correct_answers_{user_id}')

def clear_questions(users_id, r_conn):
    for user_id in users_id:
        set_last_num_of_questions(r_conn, user_id, 0)
        delete_all_questions(user_id, r_conn)
def clear_answers(users_id, r_conn):
    for user_id in users_id:
        delete_all_correct_answers(user_id, r_conn)

def clear_qa_from_dir(directory_name='questions_data'):
    path_directory = os.path.abspath(f'./{directory_name}')
    path_directory = path_directory.replace('\\', '/')
    for file_name in os.listdir(path_directory):
        os.remove(f'{path_directory}/{file_name}')

def delete_select_question(user_id, key, r_conn):
    r_conn.hdel(f'questions_id_{user_id}', key)

def delete_select_correct_answer(user_id, key, r_conn):
    r_conn.hdel(f'correct_answers_id_{user_id}', key)


def main():
    load_dotenv()
    # password_redis_db = os.getenv("REDIS_DB")
    # db_redis = redis.Redis(
    #     host='redis-12655.c299.asia-northeast1-1.gce.cloud.redislabs.com',
    #     port=12655,
    #     db=0,
    #     password=password_redis_db
    # )

    host = os.getenv('host')
    port = os.getenv('port')
    # password = 0000
    db = 0

    r_conn = redis.Redis(
        host=host,
        port=port,
        db=db,
        charset='utf-8',
        decode_responses=True
    )

    upload_all_files_of_qa(r_conn)

if __name__ =='__main__':

    main()