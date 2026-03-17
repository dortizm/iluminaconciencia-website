#!/usr/bin/python
# -*- coding: utf-8 -*-
from apps.dashboard.models import TessW, Tess4C, LastNightTessW, LastWeekTessW, LastMonthTessW, Moon, HistoricalValuesTessW
import plotly.graph_objs as go
import plotly.express as px
from datetime import datetime
from django.template.loader import render_to_string
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from influxdb_client import InfluxDBClient
from bs4 import BeautifulSoup
import smtplib
import environ
import pandas as pd
import os
import pytz
import logging
import json

class ReportsProcessing:

    def send_observation_time_report(self,data):
        html_table = data.to_html(index=False)
        soup = BeautifulSoup(html_table, 'html.parser')
        table = soup.find('table')
        table['style'] = 'border: 1px solid black; border-collapse: collapse;'
        for th in table.find_all('th'):
            th['style'] = 'text-align: left; border: 1px solid black; padding: 8px; background-color: #f2f2f2;'
        for td in table.find_all('td'):
            td['style'] = 'text-align: left; border: 1px solid black; padding: 8px;'
        html_table = str(soup)
        html_body = f"""
                    <html>
                    <head></head>
                    <body>
                      <p>Estimados</p>
                      <p>Este informe presenta el estado de los instrumentos TESS-W del proyecto, cubriendo un período de 7 días hasta la fecha.</p>
                      {html_table}
                      <p>Saludos</p>
                    </body>
                    </html>
                    """
        body_msg = MIMEText(html_body, 'html')
        msg = MIMEMultipart('alternative')
        msg.attach(body_msg)
        msg['Subject'] = 'Reporte de disponibilidad de dispositivos TESS-W iluminaconciencia'
        try:
            server = smtplib.SMTP(self.SMTP_SERVER, self.SMTP_PORT)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(self.SMTP_USERNAME, self.SMTP_PASSWORD)
            server.sendmail(self.SMTP_SENDER, self.SMTP_RECIPIENT, msg.as_string())
            server.close()
            print("Correo enviado con éxito!")
        except Exception as e:
            print("Error al enviar el correo electrónico:", e)

        print(html_body)

    def observation_time_report(self):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())

        data={'Institución':[],'Cantidad de Datos':[],'Porcentaje':[]}
        minutes_in_week= 5 * 60 * 7

        for instrument in instruments:    
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "from(bucket: \""+self.INFLUX_BUCKET+"\")\
                                    |> range(start: -7d)\
                                    |> hourSelection(start: 4, stop: 8)\
                                    |> filter(fn: (r) => r._measurement == \"mqtt_consumer\")\
                                    |> filter(fn: (r) => r[\"_field\"] == \""+mag+"\")\
                                    |> filter(fn: (r) => exists r.influxdb_tag)\
                                    |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                                    |> count(column: \"_value\")\
                                    |> map(fn: (r) => ({ _name: r[\"name\"], _value: r[\"_value\"] }))\
                    "
            result = self.client.query_api().query(query)
            if len(result) == 0:
                total_measurements_week=0
            else:
                result_json=json.loads(result.to_json())
                item = result_json[0]
                total_measurements_week=item['_value']
            data['Institución'].append(instrument.name)
            data['Cantidad de Datos'].append(total_measurements_week)
            data['Porcentaje'].append("{:.2f}%".format((total_measurements_week/minutes_in_week) * 100))
        data = pd.DataFrame(data)     
        data = data.sort_values(by='Porcentaje', ascending=False)
        self.send_observation_time_report(data)
        self.__log.info("End Observation Time Report")

    def create_night_graph(self, tess_id):
        try:
            if LastNightTessW.objects.filter(tess__id=tess_id).count() > 0:
                new_timezone = pytz.timezone('America/Santiago')
                first_update=LastNightTessW.objects.filter(tess__id=tess_id).order_by('record_time').first().record_time
                last_update=LastNightTessW.objects.filter(tess__id=tess_id).order_by('record_time').last().record_time
                df_cont_lum_last_night = pd.DataFrame(list(LastNightTessW.objects.filter(tess__id=tess_id).order_by('record_time').values('record_time','magnitude','weather')))
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

            fig_last_night_magnitude = px.scatter(df_cont_lum_last_night[df_cont_lum_last_night.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=self.color_discrete_map,
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
            fig_last_night_magnitude.write_image(f"night_plot_{tess_id}.png", format="png")
        except Exception as e:
            print(e)

    def create_week_graph(self, tess_id):
        try:
            # Se cargan los datos desde la BD en un dataframe
            if LastWeekTessW.objects.filter(tess__id=tess_id).count() > 0:
                new_timezone = pytz.timezone('America/Santiago')
                first_update=LastWeekTessW.objects.filter(tess__id=tess_id).order_by('record_time').first().record_time
                last_update=LastWeekTessW.objects.filter(tess__id=tess_id).order_by('record_time').last().record_time
                df_cont_lum_last_week = pd.DataFrame(list(LastWeekTessW.objects.filter(tess__id=tess_id).order_by('record_time').values('record_time','magnitude','weather')))
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
            
            fig_last_week_magnitud = px.scatter(df_cont_lum_last_week[df_cont_lum_last_week.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=self.color_discrete_map,
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

            fig_last_week_magnitud.write_image(f"week_plot_{tess_id}.png", format="png")
        except Exception as e:
            print(e)

    def create_month_graph(self, tess_id):
        try:
            # Se cargan los datos desde la BD en un dataframe
            if LastMonthTessW.objects.filter(tess__id=tess_id).count() > 0:
                new_timezone = pytz.timezone('America/Santiago')
                first_update=LastMonthTessW.objects.filter(tess__id=tess_id).order_by('record_time').first().record_time
                last_update=LastMonthTessW.objects.filter(tess__id=tess_id).order_by('record_time').last().record_time
                df_cont_lum_last_month = pd.DataFrame(list(LastMonthTessW.objects.filter(tess__id=tess_id).order_by('record_time').values('record_time','magnitude','weather')))
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
            
            fig_last_month_magnitud = px.scatter(df_cont_lum_last_month[df_cont_lum_last_month.weather.isin(["Nublado","Cubierto","Despejado"])], x="record_time_x", y="magnitude", color="weather",color_discrete_map=self.color_discrete_map,
                         labels={
                             "record_time_x": "Fecha",
                             "magnitude": "Brillo del cielo (mag/arcsec²)",
                             "weather": "Clima"
                         },title="Figure 2")

            # Agregar un gráfico de líneas para Variable 2 en el segundo eje Y
            fig_last_month_magnitud.add_trace(go.Scatter(x=df_cont_lum_last_month['record_time_x'], y=df_cont_lum_last_month['brightness'], name='Ciclo lunar', yaxis='y2',marker_color='rgba(213, 169, 64, 1)'))

            # Configurar los ejes Y
            fig_last_month_magnitud.update_layout(
                yaxis=dict(title='Brillo del cielo (mag/arcsec^2)', titlefont=dict(color='#150e3c'), tickfont=dict(color='#150e3c')),
                yaxis2=dict(titlefont=dict(color='#d5a940'), tickfont=dict(color='#d5a940'),
                            overlaying='y', side='right')
            )
            fig_last_month_magnitud.write_image(f"fig_2_{tess_id}.png", format="png")
        except Exception as e:
            print(e)

    def create_sky_state(self, tess_id):
        try:
            stadistics=HistoricalValuesTessW.objects.filter(tess__id=tess_id).order_by('calculation_date').last()
            variables=['Despejado','Nublado','Cubierto']
            clear=int(stadistics.total_measurements_clear/(5*60)) if stadistics.total_measurements_clear else 0
            cloudy=int(stadistics.total_measurements_cloudy/(5*60)) if stadistics.total_measurements_cloudy else 0
            covered=int(stadistics.total_measurements_covered/(5*60)) if stadistics.total_measurements_covered else 0
            data=[clear,cloudy,covered]
            colors=[self.color_discrete_map[variables[0]],self.color_discrete_map[variables[1]],self.color_discrete_map[variables[2]]]
            fig = go.Figure(data=[go.Bar(name='Cantidad de días según el estado del cielo por noche', x=variables, y=data, marker_color=colors)])
            fig.update_layout(
                xaxis_title='Estado del Cielo',
                yaxis_title='Cantidad de días',
                showlegend=False,
                title="Figure 1"
            )
            fig.write_image(f"fig_1_{tess_id}.png", format="png")
        except Exception as e:
            print(e)

    def send_month_report(self,id):
        try:
            tessw = TessW.objects.get(id=id)
            months=['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre']
            month=datetime.utcnow().strftime("%-m")
            stadistics=HistoricalValuesTessW.objects.filter(tess=tessw).order_by('calculation_date').last()
            historical=HistoricalValuesTessW.objects.filter(tess=tessw).order_by('calculation_date')
            days=int(stadistics.total_measurements_month/(5*60))
            month=months[int(month)-1]
            context = {
                'id': tessw.id,
                'month': month,
                'name': tessw.name,
                'commune': tessw.commune,
                'region': tessw.region,
                'days': days,
                'total_measurements_month': stadistics.total_measurements_month if stadistics.total_measurements_month else 0,
                'median_magnitude': round(stadistics.median_magnitude,2) if stadistics.median_magnitude else 0,
                'total_measurements_clear': stadistics.total_measurements_clear if stadistics.total_measurements_clear else 0,
                'total_measurements_cloudy': stadistics.total_measurements_cloudy if stadistics.total_measurements_cloudy else 0,
                'total_measurements_covered': stadistics.total_measurements_covered if stadistics.total_measurements_covered else 0,
                'percentage_measurements_clear': f"{round(stadistics.percentage_measurements_clear*100,2) if stadistics.percentage_measurements_clear else 0}%",
                'percentage_measurements_cloudy': f"{round(stadistics.percentage_measurements_cloudy*100,2) if stadistics.percentage_measurements_cloudy else 0}%",
                'percentage_measurements_covered': f"{round(stadistics.percentage_measurements_covered*100,2) if stadistics.percentage_measurements_covered else 0}%",
                'data_url': stadistics.data_url,
                'historical': historical,
            }
            
            html_content = render_to_string('report_email_template.html', context) 
            body_msg = MIMEText(html_content, 'html')
            msg = MIMEMultipart('alternative')
            msg.attach(body_msg)
            with open('static/images/logo_white.png', 'rb') as image_file:
                image = MIMEImage(image_file.read())
                image.add_header('Content-ID', '<logo>')  # Debe coincidir con el cid en el HTML
                image.add_header('Content-Disposition', 'inline', filename='logo.png')
                msg.attach(image)
            self.create_sky_state(tessw.id)
            file_graph=f"fig_1_{tessw.id}.png"
            with open(file_graph, 'rb') as image_file:
                image = MIMEImage(image_file.read())
                image.add_header('Content-ID', '<fig_1>')  # Debe coincidir con el cid en el HTML
                image.add_header('Content-Disposition', 'inline', filename=file_graph)
                msg.attach(image)
            os.remove(f'fig_1_{tessw.id}.png')
            self.create_month_graph(tessw.id)
            file_graph=f"fig_2_{tessw.id}.png"
            with open(file_graph, 'rb') as image_file:
                image = MIMEImage(image_file.read())
                image.add_header('Content-ID', '<fig_2>')  # Debe coincidir con el cid en el HTML
                image.add_header('Content-Disposition', 'inline', filename=file_graph)
                msg.attach(image)
            os.remove(f'fig_2_{tessw.id}.png')
            msg['Subject'] = f"IluminAConciencia - Reporte mes de {month}"
            server = smtplib.SMTP(self.SMTP_SERVER, self.SMTP_PORT)
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(self.SMTP_USERNAME, self.SMTP_PASSWORD)
            server.sendmail(self.SMTP_SENDER, tessw.institution_email, msg.as_string())
            server.close()
            print("Correo enviado con éxito!")
        except Exception as e:
            print(e)

    def __init__(self):
        env = environ.Env()
        environ.Env.read_env()
        self.SMTP_SERVER = env('SMTP_SERVER')
        self.SMTP_USERNAME = env('SMTP_USERNAME')
        self.SMTP_PASSWORD = env('SMTP_PASSWORD')
        self.SMTP_SENDER = env('SMTP_SENDER')
        self.SMTP_RECIPIENT = env('SMTP_RECIPIENT')
        self.SMTP_PORT = env('SMTP_PORT')
        self.INFLUX_BUCKET = env('INFLUX_BUCKET')
        self.__log = logging.getLogger(__name__)
        self.__log.setLevel(logging.DEBUG)
        handler = logging.FileHandler(env('LOG_FILENAME'))
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(lineno)d;%(asctime)s;%(levelname)s;%(message)s')
        handler.setFormatter(formatter)
        self.__log.addHandler(handler)
        self.client = InfluxDBClient(url=env('INFLUX_URL'), token=env('INFLUX_TOKEN'), org=env('INFLUX_ORG'), timeout=3000_000)
        self.color_discrete_map = {
        "Despejado": 'rgb(18,127,16)',
        "Nublado": 'rgb(220,53,69)',
        "Cubierto": 'rgb(14,14,117)'
        }