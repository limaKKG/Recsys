from flask import Flask, request, render_template
import pandas as pd

app = Flask(__name__)

# Загрузка данных при старте приложения
recommendations = pd.read_csv('/home/kamilg/data/catrecs_df.csv')

@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user_id = request.form['user_id']
        return redirect(url_for('recommend', user_id=user_id))
    return render_template('login.html')

@app.route('/recommend/<user_id>')
def recommend(user_id):
    # Получение рекомендаций для данного пользователя
    user_recommendations = recommendations[recommendations['user_id'] == user_id]
    user_recommendations = user_recommendations.head(10)
    # Преобразование DataFrame в список словарей
    recommendations_dict = user_recommendations.to_dict(orient='records')
    return render_template('recommend.html', recommendations=recommendations_dict)
if __name__ == "__main__":
    app.run(debug=True)
