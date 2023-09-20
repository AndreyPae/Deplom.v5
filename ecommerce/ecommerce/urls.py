from django.contrib import admin
from django.urls import path, include
from store.views import user_login, user_logout, product_list, add_to_cart, view_cart, place_order

urlpatterns = [
    path('admin/', admin.site.urls),
    path('login/', user_login, name='авторизоваться'),
    path('logout/', user_logout, name='выход'),
    path('', product_list, name='список товаров'),
    path('add_to_cart//', add_to_cart, name='добавить в корзину'),
    path('cart/', view_cart, name='просмотреть корзину'),
    path('place_order/', place_order, name='заказать'),
]