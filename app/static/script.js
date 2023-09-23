window.onload = function() {
    var items = document.getElementById('items');
    
    // Предположим, что 'data' - это ваш массив рекомендаций
    var data = {{ recommendations | tojson | safe }};
    
    for (var i = 0; i < data.length; i++) {
        var item = document.createElement('div');
        item.className = 'item';
        
        var title = document.createElement('h2');
        title.textContent = data[i].ecom_nm;
        item.appendChild(title);
        
        var likeButton = document.createElement('button');
        likeButton.textContent = 'Лайк';
        item.appendChild(likeButton);
        
        var buyButton = document.createElement('button');
        buyButton.textContent = 'Купить';
        item.appendChild(buyButton);
        
        var cartButton = document.createElement('button');
        cartButton.textContent = 'Добавить в корзину';
        item.appendChild(cartButton);
        
        items.appendChild(item);
    }
};
