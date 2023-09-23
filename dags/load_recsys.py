from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import ast
from sklearn.model_selection import train_test_split
import pandas as pd
# Функция для подключения к базе данных и извлечения данных
def get_data_from_postgres():
    # Создание подключения
    engine = create_engine('postgresql://postgres:kamilg@158.160.40.22:5432/recsys')

    # Запрос SQL для извлечения данных
    query = "SELECT * FROM wildberris_recsys"

    # Использование Dask для выполнения SQL-запроса и загрузки данных в DataFrame
    df_old = df.read_sql_table(query, engine)

    return df_old

# Функции для обработки данных и обучения модели
def parse_list(s):
    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return []

def calculate_full_price(prices, quantities):
    return sum([price * qty for price, qty in zip(prices, quantities)])

def prepare_data():
    df_old = get_data_from_postgres()
    df_old = df_old.drop_duplicates()
    df_old['ecom.price100'] = df_old['ecom.price100'].apply(parse_list)
    df_old['ecom.qty'] = df_old['ecom.qty'].apply(parse_list)
    df_old['full_price'] = df_old.apply(lambda row: calculate_full_price(row['ecom.price100'], row['ecom.qty']), axis=1)
    global_train, global_test = train_test_split(df_old, test_size = 0.3, random_state=42)
    first_level_train, first_level_test = train_test_split(global_train, test_size = 0.3, random_state=42)
    # Преобразование строк в списки
    cols_to_convert = ['ecom.price100', 'ecom.qty', 'ecom.nm', 'main_category', 'sub_category']
    expanded_rows = []

    for _, row in first_level_train.iterrows():
        for col in cols_to_convert:
            values = row[col]
            if isinstance(values, str):
                values = values.split(',')  
                for value in values:
                    new_row = row.copy()
                    new_row[col] = value
                    expanded_rows.append(new_row)
    expanded_first_train = pd.DataFrame(expanded_rows)
    # Сброс индексов
    expanded_first_train.reset_index(drop=True, inplace=True)
    # Сохранение expanded_first_train в XCom
    ti = context['ti']
    ti.xcom_push(key='expanded_first_train', value=expanded_first_train)
# Определение DAG
load_data_dag = DAG(
    'load_data',
    default_args={
        'owner': 'limakg',
        'depends_on_past': False,
        'start_date': datetime(2023, 9, 22),
    },
    description='A DAG to load and prepare data',
    schedule_interval=None,  # Запускать по требованию
)

dag = DAG(
    'prepare_and_train_model',
     default_args=default_args,
     description='A DAG to prepare data and train a model',
     schedule_interval=timedelta(days=1),
)

# Определение оператора для подготовки данных
prepare_data_operator = PythonOperator(
     task_id='prepare_data',
     python_callable=prepare_data,
     dag=dag,
)

# Запуск DAG
prepare_data_operator
