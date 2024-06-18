import json
import os
import re
import requests
import urllib3
import uuid
from typing import Any, Dict, List, Optional, Sequence, Union

from dotenv import load_dotenv
from langchain.chains.llm import LLMChain
from langchain.evaluation import load_evaluator
from langchain.evaluation.qa import QAEvalChain
from langchain.output_parsers.regex import RegexParser
from langchain.prompts import PromptTemplate
from langchain_community.chat_models.gigachat import GigaChat
from langchain_community.embeddings.gigachat import GigaChatEmbeddings
from langchain_core.callbacks import CallbackManager, Callbacks
from langchain_core.language_models import BaseLanguageModel
from langchain_core.load.dump import dumpd
from langchain_core.outputs import LLMResult
from langchain_core.output_parsers import BaseLLMOutputParser
from langchain_core.pydantic_v1 import Field
from sklearn.metrics.pairwise import cosine_similarity


def get_prompt_accuracy(question: str, correct_answer: str, answer: str) -> str:

    template = """
    Вы опытный преподаватель, специализирующийся на оценке ответов студентов на вопросы.
    
    Вы оцениваете следующий вопрос:
    {query}

    Вот реальный ответ:
    {answer}

    Вы оцениваете следующий ответ студента:
    {result}

    Оценка:
    Причина:
    Какую оценку вы предпочитаете: от 0 до 10, где 0 - самая низкая (очень низкое сходство), а 10 - самая высокая (очень высокое сходство)? Напиши причину.

    """

    prompt = PromptTemplate(input_variables=["query", "answer", "result"], template=template)

    return prompt

def get_prompt_qa(doc: str, num: int) -> str:
    template = """
    Вы преподаватель, который составляет вопросы для викторины.
    Исходя из следующего документа, пожалуйста, сформулируй {num} вопросы и ответы, основанные на этом документе.
    Каждый вопрос необходимо начинается со строки "Вопрос: ", а ответ - со строки "Ответ: ". Вопросы и ответы должны быть разделены пустой строкой.

    Пример формата:
    <Начало документа>
    ...
    <Конец документа>

    Вопрос: вопрос здесь
    Ответ: ответ здесь

    Вопрос: вопрос здесь
    Ответ: ответ здесь

    Убедись, что вопросы интересные и релевантные заданной теме, ответы краткие, четкие и однозначные. Постарайся разнообразить уровень сложности вопросов.
    Эти вопросы должны быть подробными и строго основываться на информации в документе. Число вопросов и ответов должно строго соответстовать {num}.

    <Начало документа>
    {doc}
    <Конец документа>
    """

    prompt = PromptTemplate(input_variables=["doc", "num"], template=template)
    return prompt

_QA_OUTPUT_PARSER = RegexParser(
    regex=r"Вопрос: (.*?)\n+Ответ: (.*)", output_keys=["query", "answer"]
)


class QAGenerateChain(LLMChain):
    """LLM Chain for generating examples for question answering."""

    output_parser: BaseLLMOutputParser = Field(default=_QA_OUTPUT_PARSER)
    output_key: str = "qa"

    @classmethod
    def is_lc_serializable(cls) -> bool:
        return False

    @classmethod
    def from_llm(cls, llm: BaseLanguageModel, prompt: Optional[PromptTemplate] = None, **kwargs: Any):
        """Load QA Generate Chain from LLM."""
        expected_input_vars = {"doc", "num"}
        if expected_input_vars != set(prompt.input_variables):
            raise ValueError(
                f"Input variables should be {expected_input_vars}, "
                f"but got {prompt.input_variables}"
            )
        return cls(llm=llm, prompt=prompt, **kwargs)

    def create_outputs(self, llm_result: LLMResult) -> List[Dict[str, Any]]:
        return llm_result.generations[0][0].text

    def _parse_generation(
            self, generation: List[Dict[str, str]]
    ) -> Sequence[Union[str, List[str], Dict[str, str]]]:
        if self.prompt.output_parser is not None:
            return [
                self.prompt.output_parser.parse(res[self.output_key])
                for res in generation
            ]
        else:
            return generation

    def apply(
            self, input_list: List[Dict[str, Any]], callbacks: Callbacks = None
    ) -> List[Dict[str, str]]:
        # """Utilize the LLM generate method for speed gains."""
        callback_manager = CallbackManager.configure(
            callbacks, self.callbacks, self.verbose
        )
        run_manager = callback_manager.on_chain_start(
            dumpd(self),
            {"input_list": input_list},
        )
        try:
            response = self.generate(input_list, run_manager=run_manager)
        except BaseException as e:
            run_manager.on_chain_error(e)
            raise e
        outputs = response.generations[0][0].text
        return outputs

    def apply_and_parse(
            self, input_list: List[Dict[str, Any]], callbacks: Callbacks = None
    ) -> Sequence[Union[str, List[str], Dict[str, str]]]:
        """Call apply and then parse the results."""
        # warnings.warn(
        #     "The apply_and_parse method is deprecated, "
        #     "instead pass an output parser directly to LLMChain."
        # )
        result = self.apply(input_list, callbacks=callbacks)
        return self._parse_generation(result)

def custom_evaluate_qa(answer, correct_answer, question, sber_token):
    llm = GigaChat(credentials=sber_token, verify_ssl_certs=False)
    prompt = get_prompt_accuracy(question, correct_answer, answer)
    eval_chain = QAEvalChain.from_llm(llm=llm, prompt=prompt)
    qa = [
        {
            "question": question,
            "answer": correct_answer
        }
    ]

    answer = [{"answer": answer}]

    result = eval_chain.evaluate(qa, answer, question_key="question", prediction_key="answer")
    if "Причина:" in result[0]['results']:
        grade, reasoning = result[0]['results'].split("Причина:", maxsplit=1)
    else:
        grade = result[0]['results']
        reasoning = 'Нет комментарий.'
    grade = re.findall(r'\b\d+\b', grade)
    score = int(grade[0]) / 10

    return score, reasoning

def custom_generate_qa(doc, number, sber_token):
    llm = GigaChat(credentials=sber_token, verify_ssl_certs=False)
    prompt = get_prompt_qa(doc, number)
    generate_chain = QAGenerateChain.from_llm(llm=llm, prompt=prompt)
    result = generate_chain.apply_and_parse([{"doc": doc, "num": number}])

    return result

def get_cosine_similarity(answer, correct_answer, sber_token):
    embeddings = GigaChatEmbeddings(credentials=sber_token, verify_ssl_certs=False)
    answer_emb = embeddings.embed_documents(texts=[answer])
    correct_answer_emb = embeddings.embed_documents(texts=[correct_answer])
    cos_sim = cosine_similarity(answer_emb, correct_answer_emb)
    return cos_sim

def get_similarity_score(answer, correct_answer, question, sber_token):
    llm = GigaChat(credentials=sber_token, verify_ssl_certs=False)
    evaluator = load_evaluator(
        'labeled_score_string',
        llm=llm,
        normalize_by=10
    )

    eval_result = evaluator.evaluate_strings(
        prediction=answer,
        reference=correct_answer,
        input=question,
    )

    print(eval_result)
    return eval_result['score']

def get_token(auth_token, scope='GIGACHAT_API_PERS'):
    # Создадим идентификатор UUID (36 знаков)
    rq_uid = str(uuid.uuid4())

    url = 'https://ngw.devices.sberbank.ru:9443/api/v2/oauth'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
        'RqUID': rq_uid,
        'Authorization': f'Basic {auth_token}'
    }

    payload = {
        'scope': scope
    }

    try:
        urllib3.disable_warnings()
        response = requests.post(url, headers=headers, data=payload, verify=False)
        return response.json()['access_token']
    except requests.RequestException as e:
        print(f'Ошибка: {str(e)}')
        return -1

def connect_ruzik_chat(TOKEN, request, messages=None):

    url = 'https://gigachat.devices.sberbank.ru/api/v1/chat/completions'

    # Если история диалога не предоставлена, инициализируем пустым списком
    if messages is None or len(messages) == 0:
        messages = [{
            'role': 'system',
            'content': 'Отвечай как ассистент компании Рузик. Тебя зовут Рузик-чат.'
        }]

    # Добавляем сообщение пользователя в историю диалога
    messages.append({
        "role": "user",
        "content": request
    })

    # Подготовка данных запроса в формате JSON
    payload = json.dumps({
        'model': 'GigaChat:latest',
        'messages': messages,
        'temperature': 1,  # Температура генерации
        'top_p': 0.1,  # Параметр top_p для контроля разнообразия ответов
        'n': 1,  # Количество возвращаемых ответов
        'stream': False,  # Потоковая ли передача ответов
        'max_tokens': 512,  # Максимальное количество токенов в ответе
        'repetition_penalty': 1,  # Штраф за повторения
        'update_interval': 0  # Интервал обновления (для потоковой передачи)
    })

    # Заголовки запроса
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {TOKEN}'
    }

    # Выполнение POST-запроса и возвращение ответа
    try:
        response = requests.post(url, headers=headers, data=payload, verify=False)
        response_data = response.json()

        # Добавляем ответ модели в историю диалога
        messages.append({
            'role': 'assistant',
            'content': response_data['choices'][0]['message']['content']
        })

        return response, messages

    except requests.RequestException as e:
        # Обработка исключения в случае ошибки запроса
        print(f'Произошла ошибка: {str(e)}')
        return None, messages

def main():
    load_dotenv()

    sber_token= os.getenv('sber_token')
    messages = []

    TOKEN = get_token(sber_token)

    response, messages = connect_ruzik_chat(TOKEN, 'Привет!', messages)
    response, messages = connect_ruzik_chat(TOKEN, 'Как тебя зовут?', messages)
    response, messages = connect_ruzik_chat(TOKEN, 'Как пользоваться телефоном?', messages)
    print(messages)

if __name__ =='__main__':

    main()