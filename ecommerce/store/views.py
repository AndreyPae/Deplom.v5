from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from .models import Product, Order, Category, Tag
from django.http import HttpResponse
from django.template import loader
from django.shortcuts import redirect


def user_login(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            return redirect('product_list')
        messages = ["ошибка авторизации"]
        template = loader.get_template("login.html")
        return HttpResponse(template.render({"messages": messages}, request))
    elif request.method == 'GET':
        template = loader.get_template("login.html")
        return HttpResponse(template.render({}, request))


def product_detail(request, product_id):
    product = get_object_or_404(Product, pk=product_id)
    return render(request, 'store/product_detail.html', {'product': product})


def add_product(request):
    if request.method == 'POST':
    # Получить данные из формы / Создать новый товар / Сохранить товар
        return redirect('product_list') # Редирект на страницу со списком товаров
    else:
        return render(request, 'store/add_product.html')


def edit_product(request, product_id):
    product = get_object_or_404(Product, pk=product_id)
    if request.method == 'POST':
    # Получить данные из формы / Обновить данные товара / Сохранить товар
        return redirect('product_list') # Редирект на страницу со списком товаров
    else:
        return render(request, 'store/edit_product.html', {'product': product})


def delete_product(request, product_id):
    product = get_object_or_404(Product, pk=product_id)
    if request.method == 'POST':
    # Удалить товар
        return redirect('product_list') # Редирект на страницу со списком товаров
    else:
        return render(request, 'store/delete_product.html', {'product': product})


def search_products(request):
    query = request.GET.get('query')
    # Выполнить поиск товаров по заданному запросу
    return render(request, 'store/search_results.html', {'query': query})


def filter_products(request):
    category_id = request.GET.get('category')
    tag_id = request.GET.get('tag')
    # Отфильтровать товары по выбранной категории или тегу
    return render(request, 'store/filter_results.html', {'category_id': category_id, 'tag_id': tag_id})


def category_list(request):
    categories = Category.objects.all()
    return render(request, 'store/category_list.html', {'categories': categories})


def tag_list(request):
    tags = Tag.objects.all()
    return render(request, 'store/tag_list.html', {'tags': tags})


def user_logout(request):
    logout(request)
    redirect('login')


@login_required
def product_list(request):
    products = Product.objects.all()
    template = loader.get_template("product_list.html")
    return HttpResponse(template.render({"products": products}, request))


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


