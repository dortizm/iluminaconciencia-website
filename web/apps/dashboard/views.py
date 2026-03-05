# views.py

import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
from .models import TessW, Tess4C, LastNightTessW, LastWeekTessW, LastMonthTessW, Moon, HistoricalValuesTessW, \
LastNightTess4C, LastWeekTess4C, LastMonthTess4C, HistoricalValuesTess4C
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.utils import timezone
from datetime import datetime, timedelta
import pytz
import json    
from rest_framework import viewsets, filters
from .serializers import TessWSerializer

class tessw_view_set(viewsets.ReadOnlyModelViewSet):
    queryset = TessW.objects.filter(active=True)
    serializer_class = TessWSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['name', 'commune', 'region']
    ordering_fields = ['last_update', 'median_magnitude']
    ordering = ['name']

def tessw(request, id):
    # Se abre el archivo con los datos y se cargan en un dataframe
    tessw = TessW.objects.get(id=id)
    # Mapeo de colores para cada clima
    color_discrete_map = {
    "Cubierto": 'rgb(14,14,117)',
    "Despejado": 'rgb(18,127,16)',
    "Nublado": 'rgb(220,53,69)'
    }
    if LastNightTessW.objects.filter(tess=tessw).count() > 0:
        new_timezone = pytz.timezone('America/Santiago')
        first_update=LastNightTessW.objects.filter(tess=tessw).order_by('record_time').first().record_time
        last_update=LastNightTessW.objects.filter(tess=tessw).order_by('record_time').last().record_time
        df_cont_lum_last_night = pd.DataFrame(list(LastNightTessW.objects.filter(tess=tessw).order_by('record_time').values('record_time','magnitude','weather')))
        moon=pd.DataFrame(list(Moon.objects.filter(timestamp__gte=first_update,timestamp__lte=last_update).values('timestamp','brightness').order_by('timestamp')))
        moon.rename(columns={'timestamp': 'record_time'}, inplace=True)
        moon['record_time'] = moon['record_time'].dt.tz_convert(new_timezone)
        moon['record_time_srt']=moon['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_night['record_time'] = df_cont_lum_last_night['record_time'].dt.tz_convert(new_timezone)
        df_cont_lum_last_night['record_time_srt']=df_cont_lum_last_night['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_night = pd.merge(df_cont_lum_last_night,moon,on='record_time_srt',how='inner')
    else:
        last_update=''
        df_cont_lum_last_night = pd.DataFrame({'record_time_x' : [],'magnitude' : [],'weather' : [],'brightness':[]})

    #Dibujo de gráficos de lineas

    fig_last_night_magnitude = go.Figure()

    fig_last_night_magnitude = px.scatter(df_cont_lum_last_night[df_cont_lum_last_night.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=color_discrete_map,
                 labels={
                     "record_time_x": "Hora",
                     "magnitude": "Brillo del cielo (mag/arcsec^2)",
                     "weather": "Clima del cielo"
                 },
                title="Magnitud Fotometro desde 18:00 hrs del día anterior a 08:00 hrs del día actual")

    # Agregar un gráfico de líneas para Variable 2 en el segundo eje Y
    fig_last_night_magnitude.add_trace(go.Scatter(x=df_cont_lum_last_night['record_time_x'], y=df_cont_lum_last_night['brightness'], name='Ciclo lunar', yaxis='y2',marker_color='rgba(213, 169, 64, 1)'))

    # Configurar los ejes Y
    fig_last_night_magnitude.update_layout(
        yaxis=dict(title='Brillo del cielo (mag/arcsec^2)', titlefont=dict(color='#150e3c'), tickfont=dict(color='#150e3c')),
        yaxis2=dict(titlefont=dict(color='#d5a940'), tickfont=dict(color='#d5a940'),
                    overlaying='y', side='right')
    )

    # Renderizar el gráfico en una plantilla HTML
    plot_div_last_night_magnitude = fig_last_night_magnitude.to_html(full_html=False)


    # Se cargan los datos desde la BD en un dataframe
    if LastWeekTessW.objects.filter(tess=tessw).count() > 0:
        new_timezone = pytz.timezone('America/Santiago')
        first_update=LastWeekTessW.objects.filter(tess=tessw).order_by('record_time').first().record_time
        last_update=LastWeekTessW.objects.filter(tess=tessw).order_by('record_time').last().record_time
        df_cont_lum_last_week = pd.DataFrame(list(LastWeekTessW.objects.filter(tess=tessw).order_by('record_time').values('record_time','magnitude','weather')))
        moon=pd.DataFrame(list(Moon.objects.filter(timestamp__gte=first_update,timestamp__lte=last_update).values('timestamp','brightness').order_by('timestamp')))
        moon.rename(columns={'timestamp': 'record_time'}, inplace=True)
        moon['record_time'] = moon['record_time'].dt.tz_convert(new_timezone)
        moon['record_time_srt']=moon['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_week['record_time'] = df_cont_lum_last_week['record_time'].dt.tz_convert(new_timezone)
        df_cont_lum_last_week['record_time_srt']=df_cont_lum_last_week['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_week = pd.merge(df_cont_lum_last_week,moon,on='record_time_srt',how='inner')
    else:
        df_cont_lum_last_week = pd.DataFrame({'record_time_x':[],'magnitude' : [],'weather' : [],'brightness' : []})
    
    #Dibujo de gráficos de lineas

    fig_last_week_magnitud = go.Figure()
    
    fig_last_week_magnitud = px.scatter(df_cont_lum_last_week[df_cont_lum_last_week.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=color_discrete_map,
                 labels={
                     "record_time_x": "Fecha",
                     "magnitude": "Brillo del cielo (mag/arcsec^2)",
                     "weather": "Clima"
                 },
                title="Magnitud del Fotómetro en los ultimos 7 días")

    # Agregar un gráfico de líneas para Variable 2 en el segundo eje Y
    fig_last_week_magnitud.add_trace(go.Scatter(x=df_cont_lum_last_week['record_time_x'], y=df_cont_lum_last_week['brightness'], name='Ciclo lunar', yaxis='y2',marker_color='rgba(213, 169, 64, 1)'))

    # Configurar los ejes Y
    fig_last_week_magnitud.update_layout(
        yaxis=dict(title='Brillo del cielo (mag/arcsec^2)', titlefont=dict(color='#150e3c'), tickfont=dict(color='#150e3c')),
        yaxis2=dict(titlefont=dict(color='#d5a940'), tickfont=dict(color='#d5a940'),
                    overlaying='y', side='right')
    )

    # Renderizar el gráfico en una plantilla HTML
    plot_div_last_week_magnitude = fig_last_week_magnitud.to_html(full_html=False)

    # Se cargan los datos desde la BD en un dataframe
    if LastMonthTessW.objects.filter(tess=tessw).count() > 0:
        new_timezone = pytz.timezone('America/Santiago')
        first_update=LastMonth.objects.filter(tess=tessw).order_by('record_time').first().record_time
        last_update=LastMonth.objects.filter(tess=tessw).order_by('record_time').last().record_time
        df_cont_lum_last_month = pd.DataFrame(list(LastMonth.objects.filter(tess=tessw).order_by('record_time').values('record_time','magnitude','weather')))
        moon=pd.DataFrame(list(Moon.objects.filter(timestamp__gte=first_update,timestamp__lte=last_update).values('timestamp','brightness').order_by('timestamp')))
        moon.rename(columns={'timestamp': 'record_time'}, inplace=True)
        moon['record_time'] = moon['record_time'].dt.tz_convert(new_timezone)
        moon['record_time_srt']=moon['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_month['record_time'] = df_cont_lum_last_month['record_time'].dt.tz_convert(new_timezone)
        df_cont_lum_last_month['record_time_srt']=df_cont_lum_last_month['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_month = pd.merge(df_cont_lum_last_month,moon,on='record_time_srt',how='inner')
    else:
        df_cont_lum_last_month = pd.DataFrame({'record_time_x':[],'magnitude' : [],'weather' : [],'brightness' : []})

    #Dibujo de gráficos de lineas

    fig_last_month_magnitud = go.Figure()

    fig_last_month_magnitud = px.scatter(df_cont_lum_last_month[df_cont_lum_last_month.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=color_discrete_map,
                 labels={
                     "record_time_x": "Fecha",
                     "magnitude": "Brillo del cielo (mag/arcsec^2)",
                     "weather": "Clima"
                 },
                title="Magnitud del Fotómetro en los ultimos 30 días")

    # Agregar un gráfico de líneas para Variable 2 en el segundo eje Y
    fig_last_month_magnitud.add_trace(go.Scatter(x=df_cont_lum_last_month['record_time_x'], y=df_cont_lum_last_month['brightness'], name='Ciclo lunar', yaxis='y2',marker_color='rgba(213, 169, 64, 1)'))

    # Configurar los ejes Y
    fig_last_month_magnitud.update_layout(
        yaxis=dict(title='Brillo del cielo (mag/arcsec^2)', titlefont=dict(color='#150e3c'), tickfont=dict(color='#150e3c')),
        yaxis2=dict(titlefont=dict(color='#d5a940'), tickfont=dict(color='#d5a940'),
                    overlaying='y', side='right')
    )

    # Renderizar el gráfico en una plantilla HTML
    plot_div_last_month_magnitude = fig_last_month_magnitud.to_html(full_html=False)

    historical = HistoricalValuesTessW.objects.filter(tess=tessw)

    return render(request, 'tessw.html', {'tessw':tessw, 'last_update':last_update, 'plot_div_last_night_magnitude': plot_div_last_night_magnitude, 'plot_div_last_week_magnitude': plot_div_last_week_magnitude, 'plot_div_last_month_magnitude': plot_div_last_month_magnitude, 'historical':historical})

def tess4c(request, id):
    # Se abre el archivo con los datos y se cargan en un dataframe
    tess4c = Tess4C.objects.get(id=id)
    # Mapeo de colores para cada clima
    color_discrete_map = {
    "Cubierto": 'rgb(14,14,117)',
    "Despejado": 'rgb(18,127,16)',
    "Nublado": 'rgb(220,53,69)'
    }
    if LastNightTess4C.objects.filter(tess=tess4c).count() > 0:
        new_timezone = pytz.timezone('America/Santiago')
        first_update=LastNightTess4C.objects.filter(tess=tess4c).order_by('record_time').first().record_time
        last_update=LastNightTess4C.objects.filter(tess=tess4c).order_by('record_time').last().record_time
        df_cont_lum_last_night = pd.DataFrame(list(LastNightTess4C.objects.filter(tess=tess4c).order_by('record_time').values('record_time','magnitude','weather')))
        moon=pd.DataFrame(list(Moon.objects.filter(timestamp__gte=first_update,timestamp__lte=last_update).values('timestamp','brightness').order_by('timestamp')))
        moon.rename(columns={'timestamp': 'record_time'}, inplace=True)
        moon['record_time'] = moon['record_time'].dt.tz_convert(new_timezone)
        moon['record_time_srt']=moon['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_night['record_time'] = df_cont_lum_last_night['record_time'].dt.tz_convert(new_timezone)
        df_cont_lum_last_night['record_time_srt']=df_cont_lum_last_night['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_night = pd.merge(df_cont_lum_last_night,moon,on='record_time_srt',how='inner')
    else:
        last_update=''
        df_cont_lum_last_night = pd.DataFrame({'record_time_x' : [],'magnitude' : [],'weather' : [],'brightness':[]})

    #Dibujo de gráficos de lineas

    fig_last_night_magnitude = go.Figure()

    fig_last_night_magnitude = px.scatter(df_cont_lum_last_night[df_cont_lum_last_night.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=color_discrete_map,
                 labels={
                     "record_time_x": "Hora",
                     "magnitude": "Brillo del cielo (mag/arcsec^2)",
                     "weather": "Clima del cielo"
                 },
                title="Magnitud Fotometro desde 18:00 hrs del día anterior a 08:00 hrs del día actual")

    # Agregar un gráfico de líneas para Variable 2 en el segundo eje Y
    fig_last_night_magnitude.add_trace(go.Scatter(x=df_cont_lum_last_night['record_time_x'], y=df_cont_lum_last_night['brightness'], name='Ciclo lunar', yaxis='y2',marker_color='rgba(213, 169, 64, 1)'))

    # Configurar los ejes Y
    fig_last_night_magnitude.update_layout(
        yaxis=dict(title='Brillo del cielo (mag/arcsec^2)', titlefont=dict(color='#150e3c'), tickfont=dict(color='#150e3c')),
        yaxis2=dict(titlefont=dict(color='#d5a940'), tickfont=dict(color='#d5a940'),
                    overlaying='y', side='right')
    )

    # Renderizar el gráfico en una plantilla HTML
    plot_div_last_night_magnitude = fig_last_night_magnitude.to_html(full_html=False)


    # Se cargan los datos desde la BD en un dataframe
    if LastWeekTess4C.objects.filter(tess=tess4c).count() > 0:
        new_timezone = pytz.timezone('America/Santiago')
        first_update=LastWeekTess4C.objects.filter(tess=tess4c).order_by('record_time').first().record_time
        last_update=LastWeekTess4C.objects.filter(tess=tess4c).order_by('record_time').last().record_time
        df_cont_lum_last_week = pd.DataFrame(list(LastWeekTess4C.objects.filter(tess=tess4c).order_by('record_time').values('record_time','magnitude','weather')))
        moon=pd.DataFrame(list(Moon.objects.filter(timestamp__gte=first_update,timestamp__lte=last_update).values('timestamp','brightness').order_by('timestamp')))
        moon.rename(columns={'timestamp': 'record_time'}, inplace=True)
        moon['record_time'] = moon['record_time'].dt.tz_convert(new_timezone)
        moon['record_time_srt']=moon['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_week['record_time'] = df_cont_lum_last_week['record_time'].dt.tz_convert(new_timezone)
        df_cont_lum_last_week['record_time_srt']=df_cont_lum_last_week['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_week = pd.merge(df_cont_lum_last_week,moon,on='record_time_srt',how='inner')
    else:
        df_cont_lum_last_week = pd.DataFrame({'record_time_x':[],'magnitude' : [],'weather' : [],'brightness' : []})
    
    #Dibujo de gráficos de lineas

    fig_last_week_magnitud = go.Figure()
    
    fig_last_week_magnitud = px.scatter(df_cont_lum_last_week[df_cont_lum_last_week.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=color_discrete_map,
                 labels={
                     "record_time_x": "Fecha",
                     "magnitude": "Brillo del cielo (mag/arcsec^2)",
                     "weather": "Clima"
                 },
                title="Magnitud del Fotómetro en los ultimos 7 días")

    # Agregar un gráfico de líneas para Variable 2 en el segundo eje Y
    fig_last_week_magnitud.add_trace(go.Scatter(x=df_cont_lum_last_week['record_time_x'], y=df_cont_lum_last_week['brightness'], name='Ciclo lunar', yaxis='y2',marker_color='rgba(213, 169, 64, 1)'))

    # Configurar los ejes Y
    fig_last_week_magnitud.update_layout(
        yaxis=dict(title='Brillo del cielo (mag/arcsec^2)', titlefont=dict(color='#150e3c'), tickfont=dict(color='#150e3c')),
        yaxis2=dict(titlefont=dict(color='#d5a940'), tickfont=dict(color='#d5a940'),
                    overlaying='y', side='right')
    )

    # Renderizar el gráfico en una plantilla HTML
    plot_div_last_week_magnitude = fig_last_week_magnitud.to_html(full_html=False)



    # Se cargan los datos desde la BD en un dataframe
    if LastMonthTess4C.objects.filter(tess=tess4c).count() > 0:
        new_timezone = pytz.timezone('America/Santiago')
        first_update=LastMonthTess4C.objects.filter(tess=tess4c).order_by('record_time').first().record_time
        last_update=LastMonthTess4C.objects.filter(tess=tess4c).order_by('record_time').last().record_time
        df_cont_lum_last_month = pd.DataFrame(list(LastMonthTess4C.objects.filter(tess=tess4c).order_by('record_time').values('record_time','magnitude','weather')))
        moon=pd.DataFrame(list(Moon.objects.filter(timestamp__gte=first_update,timestamp__lte=last_update).values('timestamp','brightness').order_by('timestamp')))
        moon.rename(columns={'timestamp': 'record_time'}, inplace=True)
        moon['record_time'] = moon['record_time'].dt.tz_convert(new_timezone)
        moon['record_time_srt']=moon['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_month['record_time'] = df_cont_lum_last_month['record_time'].dt.tz_convert(new_timezone)
        df_cont_lum_last_month['record_time_srt']=df_cont_lum_last_month['record_time'].dt.strftime('%Y-%m-%d %H:%M')
        df_cont_lum_last_month = pd.merge(df_cont_lum_last_month,moon,on='record_time_srt',how='inner')
    else:
        df_cont_lum_last_month = pd.DataFrame({'record_time_x':[],'magnitude' : [],'weather' : [],'brightness' : []})
    
    #Dibujo de gráficos de lineas

    fig_last_month_magnitud = go.Figure()
    
    fig_last_month_magnitud = px.scatter(df_cont_lum_last_month[df_cont_lum_last_month.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=color_discrete_map,
                 labels={
                     "record_time_x": "Fecha",
                     "magnitude": "Brillo del cielo (mag/arcsec^2)",
                     "weather": "Clima"
                 },
                title="Magnitud del Fotómetro en los ultimos 30 días")

    # Agregar un gráfico de líneas para Variable 2 en el segundo eje Y
    fig_last_month_magnitud.add_trace(go.Scatter(x=df_cont_lum_last_month['record_time_x'], y=df_cont_lum_last_month['brightness'], name='Ciclo lunar', yaxis='y2',marker_color='rgba(213, 169, 64, 1)'))

    # Configurar los ejes Y
    fig_last_month_magnitud.update_layout(
        yaxis=dict(title='Brillo del cielo (mag/arcsec^2)', titlefont=dict(color='#150e3c'), tickfont=dict(color='#150e3c')),
        yaxis2=dict(titlefont=dict(color='#d5a940'), tickfont=dict(color='#d5a940'),
                    overlaying='y', side='right')
    )

    # Renderizar el gráfico en una plantilla HTML
    plot_div_last_month_magnitude = fig_last_month_magnitud.to_html(full_html=False)

    historical = HistoricalValuesTess4C.objects.filter(tess=tess4c)
    
    return render(request, 'tessw.html', {'tessw':tess4c, 'last_update':last_update, 'plot_div_last_night_magnitude': plot_div_last_night_magnitude, 'plot_div_last_week_magnitude': plot_div_last_week_magnitude, 'plot_div_last_month_magnitude': plot_div_last_month_magnitude, 'historical':historical})


@csrf_exempt
@require_POST
def set_tess_w_state(request):
    data={'result':'Error'}
    if request.method == 'POST':
        tessw_id = request.POST['tessw_id']
        status = request.POST['status']
        tessw=TessW.objects.get(id=tessw_id)
        tessw.status=int(status)
        if status == '0':
            last_update = request.POST['last_update']
            last_frequency = request.POST['frequency']
            last_magnitude = request.POST['magnitude']
            last_ambient_temperature = request.POST['ambient_temperature']
            last_sky_temperature = request.POST['sky_temperature']
            tessw.last_update=last_update
            active=True
        else:
            last_frequency=None
            last_magnitude=None
            last_ambient_temperature=None
            last_sky_temperature=None
            active=False
        tessw.last_frequency=last_frequency
        tessw.last_magnitude=last_magnitude
        tessw.last_ambient_temperature=last_ambient_temperature
        tessw.last_sky_temperature=last_sky_temperature
        tessw.active=active
        tessw.save()
        data={'result':'OK','status':status}
    return HttpResponse(json.dumps(data), content_type="application/json")

@csrf_exempt
@require_POST
def set_tess_4c_state(request):
    data={'result':'Error'}
    if request.method == 'POST':
        tess4c_id = request.POST['tess4c_id']
        status = request.POST['status']
        tess4c=Tess4C.objects.get(id=tess4c_id)
        tess4c.status=int(status)
        if status == '0':
            last_update = request.POST['last_update']
            last_frequency = request.POST['frequency']
            last_magnitude = request.POST['magnitude']
            last_frequency_f2 = request.POST['f2_frequency']
            last_magnitude_f2 = request.POST['f2_magnitude']
            last_frequency_f3 = request.POST['f3_frequency']
            last_magnitude_f3 = request.POST['f3_magnitude']
            last_frequency_f4 = request.POST['f4_frequency']
            last_magnitude_f4 = request.POST['f4_magnitude']
            last_ambient_temperature = request.POST['ambient_temperature']
            last_sky_temperature = request.POST['sky_temperature']
            tessw.last_update=last_update
            active=True
        else:
            last_frequency=None
            last_magnitude=None
            last_frequency_f2 = None
            last_magnitude_f2 = None
            last_frequency_f3 = None
            last_magnitude_f3 = None
            last_frequency_f4 = None
            last_magnitude_f4 = None
            last_ambient_temperature=None
            last_sky_temperature=None
            active=False
        tess4c.last_frequency=last_frequency
        tess4c.last_magnitude=last_magnitude
        tess4c.last_frequency_f2=last_frequency_f2
        tess4c.last_magnitude_f2=last_magnitude_f2
        tess4c.last_frequency_f3=last_frequency_f3
        tess4c.last_magnitude_f3=last_magnitude_f3
        tess4c.last_frequency_f4=last_frequency_f4
        tess4c.last_magnitude_f4=last_magnitude_f4
        tess4c.last_ambient_temperature=last_ambient_temperature
        tess4c.last_sky_temperature=last_sky_temperature
        tess4c.active=active
        tess4c.save()
        data={'result':'OK','status':status}
    return HttpResponse(json.dumps(data), content_type="application/json")
