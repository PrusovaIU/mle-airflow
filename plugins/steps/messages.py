from airflow.models import Variable
from airflow.providers.telegram.hooks.telegram import TelegramHook


def _get_telegram_hook():
    """Хук создаётся из значений, хранящихся в Airflow Variables."""
    token = Variable.get('telegram_token')
    chat_id = Variable.get('telegram_chat_id')
    return TelegramHook(token=token, chat_id=chat_id), chat_id


def send_telegram_success_message(context):
    hook, chat_id = _get_telegram_hook()
    
    dag = context['dag']
    dag_id = dag if isinstance(dag, str) else dag.dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag_id} с id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': chat_id,
        'text': message
    })


def send_telegram_failure_message(context):
    hook, chat_id = _get_telegram_hook()
    
    dag = context['dag']
    dag_id = dag if isinstance(dag, str) else dag.dag_id
    run_id = context['run_id']
    task_instance_key_str = context.get('task_instance_key_str', 'неизвестно')
    
    message = (
        f'Исполнение DAG {dag_id} с id={run_id} завершилось неудачно!\n'
        f'Упавшая таска: {task_instance_key_str}'
    )
    hook.send_message({
        'chat_id': chat_id,
        'text': message
    })