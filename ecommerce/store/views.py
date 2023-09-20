from urllib import request
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from .models import Product, Order
from django.http import HttpResponse
from django.template import loader


def user_login(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        user = authenticate(request, username=username, password=password)

    if user is not None:
        login(request, user)
        redirect('список продуктов')
        return HttpResponse()
    else:
        messages.error(request, 'Неправильное имя пользователя или пароль.')
        render(request, 'store/login.html')
        template = loader.get_template("templates/login.html")
        context = {
            "latest_question_list": user,
        }
        return HttpResponse(template.render(context, request))


def user_logout(request):
    logout(request)
    redirect('login')


@login_required
def product_list(request):
    products = Product.objects.all()
    render(request, 'store/product_list.html', dict(products=products))


@login_required
def add_to_cart(request, product_id):
    product = Product.objects.get(pk=product_id)
    request.user.cart.products.add(product)
    messages.success(request, 'Товар добавлен в корзину.')
    redirect('список продуктов')


@login_required
def view_cart(request):
    cart = request.user.cart
    render(request, 'store/cart.html', {'Корзина': cart})


@login_required
def place_order(request):
    cart = request.user.cart
    total_price = sum(product.price for product in cart.products.all())
    order = Order.objects.create(user=request.user, total_price=total_price)
    order.products.set(cart.products.all())
    cart.products.clear()
    messages.success(request, 'Заказ успешно размещен.')
    redirect('список продуктов')