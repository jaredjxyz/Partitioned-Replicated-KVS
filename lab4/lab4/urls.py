"""lab2 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from skvs import views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^kvs$', views.process_remote),
    url(r'^kvs/update_view', views.view_change),
    url(r'^kvs/view_update', views.view_change),
    url(r'^kvs/gossip', views.gossip),
    url(r'^kvs/get_partition_id', views.get_partition_id),
    url(r'^kvs/get_partition_members', views.get_partition_members),
    url(r'^kvs/get_all_partition_ids', views.get_all_partition_ids),
    url(r'^kvs/(?P<key>[a-zA-Z0-9_]{1,250})$', views.kvs_response),
    url(r'^kvs/(?P<key>[a-zA-Z0-9_]){251,}$', views.bad_key_response),
    url(r'^payload', views.payload_update),
    url(r'^get_simple', views.get_simple),
    url(r'^broadcast_put', views.broadcast_put),
]
