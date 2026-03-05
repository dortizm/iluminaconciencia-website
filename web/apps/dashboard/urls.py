# -*- coding: utf-8 -*-
from . import views
from django.urls import re_path, path, include
from rest_framework.routers import DefaultRouter
from django.conf import settings

router = DefaultRouter()
router.register(r'tessw_api', views.tessw_view_set, basename='tessw_api')

urlpatterns = [
    re_path(r'^tessw/([-\w\d]+)/$', views.tessw, name='tessw'),
    re_path(r'^tess4c/([-\w\d]+)/$', views.tess4c, name='tess4c'),
    re_path(r'^set_tess_w_state$', views.set_tess_w_state, name='set_tess_w_state'),
    re_path(r'^set_tess_4c_state$', views.set_tess_4c_state, name='set_tess_4c_state'),
    path('', include(router.urls))
]