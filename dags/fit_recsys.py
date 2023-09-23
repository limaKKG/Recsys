from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variablefrom airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import implicit
import numpy as np
from scipy.sparse import csr_matrix
from tqdm import tqdm
from sklearn.model_selection import train_test_split
from catboost import CatBoostClassifier

default_args = {
    'owner': 'limakg',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 22),
}

dag = DAG(
    'recommendation_model',
    default_args=default_args,
    description='A DAG for recommendation model training and ranking',
    schedule_interval=None,  # Запускать по требованию
)

# Функция для получения expanded_first_train из XCom
def get_expanded_first_train(**context):
    ti = context['ti']
    expanded_first_train = ti.xcom_pull(task_ids='prepare_and_train_model', key='expanded_first_train')
    return expanded_first_train

# Функция для подготовки и обучения модели
def prepare_and_train_model():
    def eval_list(value):
        if isinstance(value, str):
            return eval(value)
        else:
            return value 
        
    def recommend(user_id, model, user_item_matrix, N=10):
        user_index = user_to_index[user_id]
        scores = model.user_factors[user_index] @ model.item_factors.T
        top_item_indices = np.argpartition(scores, -N)[-N:]
        top_item_ids = [index_to_item[index] for index in top_item_indices]
        return top_item_ids

    expanded_first_train = get_expanded_first_train()
    # Создание словаря для пользователей и их покупок
    user_purchases = {}
    for index, row in expanded_first_train.iterrows():
        user_id = row['user_id']
        purchase = row['ecom.nm']
        if user_id not in user_purchases:
            user_purchases[user_id] = []
        user_purchases[user_id].append(purchase)
    # Создание словаря, где ключами будут user_id, а значениями - списки уникальных id товаров
    user_item_dict = {user: list(set(items)) for user, items in user_purchases.items()}
    all_items = set(item for items in user_item_dict.values() for item in items)
    # Создание обратного словаря для быстрого поиска индексов товаров по id
    item_to_index = {item: index for index, item in enumerate(all_items)}
    index_to_item = {index: item for item, index in item_to_index.items()}
    # Создание разреженной матрицы user-item
    num_users = len(user_item_dict)
    num_items = len(all_items)
    data = []
    row_indices = []
    col_indices = []
    for user_index, (user, items) in enumerate(user_item_dict.items()):
        for item in items:
            data.append(1)
            row_indices.append(user_index)
            col_indices.append(item_to_index[item])
    user_item_matrix = csr_matrix((data, (row_indices, col_indices)), shape=(num_users, num_items))
    alpha_val = 34
    data_conf = (user_item_matrix * alpha_val).astype('double')  
    # Инициализация модели
    model = implicit.als.AlternatingLeastSquares(factors=200, regularization=0.7, iterations=20)
    EPOCHS = 20
    # Обучение модели
    for _ in tqdm(range(EPOCHS), total=EPOCHS):
        model.fit(data_conf) 
    item_name_mapper = {}
    for ecom_nm_list, main_category_list in zip(expanded_first_train['ecom.nm'], expanded_first_train['main_category']):
        for ecom_nm, main_category in zip(ecom_nm_list, main_category_list):
            item_name_mapper[ecom_nm] = main_category
    test_user_ids = expanded_first_train['user_id'].unique()
    recommendations = []
    for user_id in tqdm(test_user_ids):
        top_items = recommend(user_id, model, user_item_matrix)
        recommendations.extend([(user_id, item_id) for item_id in top_items])
    # Создание датафрейма дял рекомендаций
    recs_df = pd.DataFrame(recommendations, columns=['user_id', 'item_id'])
    recs_df['rank'] = recs_df.groupby('user_id').cumcount() + 1
    recs_df['main_category'] = recs_df['item_id'].map(item_name_mapper)
    item_subtitle_mapper = {}
    for ecom_nm_list, sub_category_list in zip(['ecom.nm'], expanded_first_train['sub_category']):
        for ecom_nm, sub_category in zip(ecom_nm_list, sub_category_list):
            item_subtitle_mapper[ecom_nm] = sub_category
    recs_df['sub_category'] = recs_df['item_id'].map(item_subtitle_mapper)
    context['ti'].xcom_push(key='recs_df', value=recs_df)
       
# Функция для ранжирования рекомендаций
def rank_recommendations():
    expanded_first_train = get_expanded_first_train()
    ti = context['ti']
    recs_df = ti.xcom_pull(task_ids='prepare_and_train_model', key='recs_df')
    merged_df = pd.merge(expanded_first_train, df_als, how='inner', on=['user_id', 'ecom.nm'])
    merged_df['day_of_week'] = merged_df['utc_event_date'].dt.dayofweek
    categorical_cols = ['platform', 'event_type', 'main_category', 'sub_category']
    for col in categorical_cols:
        merged_df[col] = merged_df[col].astype('category')
    X = merged_df.drop('rank', axis=1)
    y = merged_df['rank']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    second_level_model = CatBoostClassifier(iterations=5000, 
                                            learning_rate=0.1, 
                                            depth=7, 
                                            loss_function='MultiClass', 
                                            eval_metric='Accuracy', 
                                            random_seed=42, 
                                            verbose=False, 
                                            cat_features=categorical_cols)
    second_level_model.fit(X_train, y_train)
    preds_class = model.predict(X_test)
    # Создание новой таблицы с user_id, item_id и предсказаниями
    catrecs_df = merged_df[['user_id', 'item_id']].copy()
    catrecs_df['predictions'] = preds_class
    catrecs_df.to_csv('/data/cat_recs.csv', index=False)

# Определение оператора для получения expanded_first_train из XCom
get_expanded_first_train_operator = PythonOperator(
    task_id='get_expanded_first_train',
    python_callable=get_expanded_first_train,
    provide_context=True,
    dag=dag,
)

# Определение оператора для подготовки и обучения модели
prepare_and_train_model_operator = PythonOperator(
    task_id='prepare_and_train_model',
    python_callable=prepare_and_train_model,
    dag=dag,
)

# Определение оператора для ранжирования рекомендаций
rank_recommendations_operator = PythonOperator(
    task_id='rank_recommendations',
    python_callable=rank_recommendations,
    dag=dag,
)

# Определение зависимостей между задачами
get_expanded_first_train_operator >> prepare_and_train_model_operator
prepare_and_train_model_operator >> rank_recommendations_operator
