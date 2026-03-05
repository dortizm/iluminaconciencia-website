# -*- coding: utf-8 -*-

from django.urls import re_path
from . import views
from django.conf import settings

urlpatterns = [
    re_path(r'^$', views.index, name='index'),
    re_path(r'^team$', views.team, name='team'),
    re_path(r'^multimedia$', views.multimedia, name='multimedia'),
    re_path(r'^news_item/(\d+)/$', views.news_item, name='news_item'),
    re_path(r'^news_list$', views.news_list, name='news_list'),
    re_path(r'^solicitud_informe$', views.solicitar_informe, name='solicitar_informe'),
    re_path(r'^galeria$', views.galeria_fotos, name='galeria'),
]