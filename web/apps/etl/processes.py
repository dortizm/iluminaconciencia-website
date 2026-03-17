#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import configparser
from apps.dashboard.models import TessW, Tess4C, LastNightTessW, LastWeekTessW, LastMonthTessW, HistoricalValuesTessW, HistoricalValuesTess4C, \
LastNightTess4C, LastWeekTess4C, LastMonthTess4C, Instrument
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta
import pytz
import json
import numpy as np
import environ
import requests
import boto3
import os

class ETLProcessing:

    def load_last_night(self, hours=14, time_stop="12:00:00"):
        self.__log.info("Start Load last night")
        today=datetime.now().strftime("%Y-%m-%d")
        to_date = datetime.strptime(today+" "+time_stop, "%Y-%m-%d %H:%M:%S").isoformat() + "Z"
        from_date = (datetime.strptime(to_date, "%Y-%m-%dT%H:%M:%SZ")-timedelta(hours=hours)).isoformat() + "Z"
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        LastNightTessW.objects.all().delete()
        LastNightTess4C.objects.all().delete()
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "from(bucket: \""+self.INFLUX_BUCKET+"\")\
                    |> range(start: "+from_date+", stop: "+to_date+")\
                    |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                    |> filter(fn: (r) => r[\"_field\"] == \""+mag+"\" or r[\"_field\"] == \"tamb\" or r[\"_field\"] == \"tsky\")\
                    |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                    |> aggregateWindow(every: 5m, fn: median, createEmpty: true)\
                    |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                    |> map(fn: (r) => ({r with\
                        temp: r.tamb-r.tsky}))\
                    |> map(fn: (r) => ({r with \
                        weather: if r.temp < 11.66 then\
                                  \"Cubierto\"\
                                else if  r.temp>= 11.66 and r.temp <= 20 then\
                                  \"Nublado\"\
                                else\
                                  \"Despejado\",\
                        })\
                    )\
            "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())            
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    record_time=item['_time'] #'tiempo_lectura_nodo'
                    record_time = datetime.fromisoformat(record_time)
                    record_time = record_time.astimezone(pytz.utc)
                    magnitude=item[mag] #'magnitude'
                    sky_temperature=item['tsky'] #'sky_temperature'
                    ambient_temperature=item['tamb'] #'ambient_temperature'
                    weather=item['weather'] #weather
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        last_night=LastNightTessW(tess=tess,record_time=record_time,ambient_temperature=ambient_temperature,magnitude=magnitude,sky_temperature=sky_temperature,weather=weather)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        last_night=LastNightTess4C(tess=tess,record_time=record_time,ambient_temperature=ambient_temperature,magnitude=magnitude,sky_temperature=sky_temperature,weather=weather)
                    last_night.save()
                    self.__log.info("End Load last night Tess "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def load_last_week(self, days=7, time_stop="12:00:00"):
        self.__log.info("Start Load last week")
        today=datetime.now().strftime("%Y-%m-%d")
        to_date = datetime.strptime(today+" "+time_stop, "%Y-%m-%d %H:%M:%S").isoformat() + "Z"
        from_date = (datetime.strptime(to_date, "%Y-%m-%dT%H:%M:%SZ")-timedelta(days=days)).isoformat() + "Z"
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        LastWeekTessW.objects.all().delete()
        LastWeekTess4C.objects.all().delete()
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "mag=from(bucket: \""+self.INFLUX_BUCKET+"\")\
                            |> range(start: "+from_date+", stop: "+to_date+")\
                            |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                            |> filter(fn: (r) => r[\"_field\"] == \""+mag+"\")\
                            |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                            |> aggregateWindow(every: 30m, fn: median, createEmpty: true)\
                    tamb=from(bucket: \""+self.INFLUX_BUCKET+"\")\
                            |> range(start: "+from_date+", stop: "+to_date+")\
                            |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                            |> filter(fn: (r) => r[\"_field\"] == \"tamb\")\
                            |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                            |> aggregateWindow(every: 30m, fn: median, createEmpty: true)\
                    tsky=from(bucket: \""+self.INFLUX_BUCKET+"\")\
                            |> range(start: "+from_date+", stop: "+to_date+")\
                            |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                            |> filter(fn: (r) => r[\"_field\"] == \"tsky\")\
                            |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                            |> aggregateWindow(every: 30m, fn: median, createEmpty: true)\
                    union(tables: [mag, tamb, tsky])\
                            |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                            |> map(fn: (r) => ({r with\
                                temp: r.tamb-r.tsky}))\
                            |> map(fn: (r) => ({r with \
                                weather: if r.temp < 11.66 then\
                                          \"Cubierto\"\
                                        else if  r.temp>= 11.66 and r.temp <= 20 then\
                                          \"Nublado\"\
                                        else\
                                          \"Despejado\",\
                                })\
                            )\
                    "

            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    record_time=item['_time'] #'tiempo_lectura_nodo'
                    record_time = datetime.fromisoformat(record_time)
                    record_time = record_time.astimezone(pytz.utc)
                    magnitude=item[mag] #'magnitude'
                    sky_temperature=item['tsky'] #'sky_temperature'
                    ambient_temperature=item['tamb'] #'ambient_temperature'
                    weather=item['weather'] #weather
                    if device_type==1:
                        tessw=TessW.objects.get(id=tess_id)
                        last_week=LastWeekTessW(tess=tessw,record_time=record_time,ambient_temperature=ambient_temperature,magnitude=magnitude,sky_temperature=sky_temperature,weather=weather)
                    elif device_type==2:
                        tess4c=Tess4C.objects.get(id=tess_id)
                        last_week=LastWeekTess4C(tess=tess4c,record_time=record_time,ambient_temperature=ambient_temperature,magnitude=magnitude,sky_temperature=sky_temperature,weather=weather)   
                    last_week.save()
                    self.__log.info("End Load last week Tess "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def load_last_month(self, days=30, time_stop="12:00:00"):
        self.__log.info("Start Load last month")
        today=datetime.now().strftime("%Y-%m-%d")
        to_date = datetime.strptime(today+" "+time_stop, "%Y-%m-%d %H:%M:%S").isoformat() + "Z"
        from_date = (datetime.strptime(to_date, "%Y-%m-%dT%H:%M:%SZ")-timedelta(days=days)).isoformat() + "Z"
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        LastMonthTessW.objects.all().delete()
        LastMonthTess4C.objects.all().delete()
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "mag=from(bucket: \""+self.INFLUX_BUCKET+"\")\
                            |> range(start: "+from_date+", stop: "+to_date+")\
                            |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                            |> filter(fn: (r) => r[\"_field\"] == \""+mag+"\")\
                            |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                            |> aggregateWindow(every: 2h, fn: median, createEmpty: true)\
                    tamb=from(bucket: \""+self.INFLUX_BUCKET+"\")\
                            |> range(start: "+from_date+", stop: "+to_date+")\
                            |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                            |> filter(fn: (r) => r[\"_field\"] == \"tamb\")\
                            |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                            |> aggregateWindow(every: 2h, fn: median, createEmpty: true)\
                    tsky=from(bucket: \""+self.INFLUX_BUCKET+"\")\
                            |> range(start: "+from_date+", stop: "+to_date+")\
                            |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                            |> filter(fn: (r) => r[\"_field\"] == \"tsky\")\
                            |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                            |> aggregateWindow(every: 2h, fn: median, createEmpty: true)\
                    union(tables: [mag, tamb, tsky])\
                            |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                            |> map(fn: (r) => ({r with\
                                temp: r.tamb-r.tsky}))\
                            |> map(fn: (r) => ({r with \
                                weather: if r.temp < 11.66 then\
                                          \"Cubierto\"\
                                        else if  r.temp>= 11.66 and r.temp <= 20 then\
                                          \"Nublado\"\
                                        else\
                                          \"Despejado\",\
                                })\
                            )\
                    "

            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    record_time=item['_time'] #'tiempo_lectura_nodo'
                    record_time = datetime.fromisoformat(record_time)
                    record_time = record_time.astimezone(pytz.utc)
                    magnitude=item[mag] #'magnitude'
                    sky_temperature=item['tsky'] #'sky_temperature'
                    ambient_temperature=item['tamb'] #'ambient_temperature'
                    weather=item['weather'] #weather
                    if device_type==1:
                        tessw=TessW.objects.get(id=tess_id)
                        last_month=LastMonthTessW(tess=tessw,record_time=record_time,ambient_temperature=ambient_temperature,magnitude=magnitude,sky_temperature=sky_temperature,weather=weather)
                    elif device_type==2:
                        tess4c=Tess4C.objects.get(id=tess_id)
                        last_month=LastMonthTess4C(tess=tess4c,record_time=record_time,ambient_temperature=ambient_temperature,magnitude=magnitude,sky_temperature=sky_temperature,weather=weather) 
                    last_month.save()
                    self.__log.info("End Load last month Tess "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass
            

    def process(self):
        self.__log.info("Load data from InfluxDB")
        self.load_last_night(14,"12:00:00")
        self.load_last_week(7,"12:00:00")
        self.load_last_month(30,"12:00:00")
        self.__log.info("Load Finish")

    def light_pollution_indicator(self):
        self.__log.info("Start Load Light Pollution Indicator")
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                            from(bucket: \""+self.INFLUX_BUCKET+"\")\
                                  |> range(start: -30d)\
                                  |> hourSelection(start: 4, stop: 8)\
                                  |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                  |> filter(fn: (r) => r[\"_field\"] == \""+mag+"\")\
                                  |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                                  |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                     moon =\
                            from(bucket: \"iluminaconciencia-luna\")\
                                  |> range(start: -30d)\
                                  |> hourSelection(start: 4, stop: 8)\
                                  |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                  |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                                  |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                                  |> filter(fn: (r) => r[\"_value_moon\"] == 0)\
                                  |> median(column: \"_value_tess\")\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            ranges = np.array([0, 18, 18.5 , 19.25, 20.3, 21.3, 21.6, 21.75, 22])
            scale = np.array([8, 7, 6, 5, 4, 3, 2, 1])
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    median_magnitude=item['_value_tess'] #'magnitude'
                    start_time_median_magnitude=item['_start_tess'] #'fecha inicio medición'
                    start_time_median_magnitude = datetime.fromisoformat(start_time_median_magnitude)
                    start_time_median_magnitude = start_time_median_magnitude.astimezone(pytz.utc)
                    stop_time_median_magnitude=item['_stop_tess'] #'magnitude'
                    stop_time_median_magnitude = datetime.fromisoformat(stop_time_median_magnitude)
                    stop_time_median_magnitude = stop_time_median_magnitude.astimezone(pytz.utc)
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id) 
                    tess=TessW.objects.get(id=tess_id)
                    tess.median_magnitude=median_magnitude
                    tess.start_time_median_magnitude=start_time_median_magnitude
                    tess.stop_time_median_magnitude=stop_time_median_magnitude
                    tess.bortle_level = scale[np.digitize(median_magnitude, ranges) - 1]
                    tess.save()
                    self.__log.info("End Load last night "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass
            

    def historical_bortle(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                            from(bucket: \""+self.INFLUX_BUCKET+"\")\
                                  |> range(start: "+from_date+", stop: "+to_date+")\
                                  |> hourSelection(start: 4, stop: 8)\
                                  |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                  |> filter(fn: (r) => r[\"_field\"] == \""+mag+"\")\
                                  |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                                  |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                     moon =\
                            from(bucket: \"iluminaconciencia-luna\")\
                                  |> range(start: "+from_date+", stop: "+to_date+")\
                                  |> hourSelection(start: 4, stop: 8)\
                                  |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                  |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                                  |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                                  |> filter(fn: (r) => r[\"_value_moon\"] == 0)\
                                  |> median(column: \"_value_tess\")\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            ranges = np.array([0, 18, 18.5 , 19.25, 20.3, 21.3, 21.6, 21.75, 22])
            scale = np.array([8, 7, 6, 5, 4, 3, 2, 1])
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    median_magnitude=item['_value_tess'] #'magnitude'
                    start_time_median_magnitude=item['_start_tess'] #'fecha inicio medición'
                    start_time_median_magnitude = datetime.fromisoformat(start_time_median_magnitude)
                    start_time_median_magnitude = start_time_median_magnitude.astimezone(pytz.utc)
                    stop_time_median_magnitude=item['_stop_tess'] #'magnitude'
                    stop_time_median_magnitude = datetime.fromisoformat(stop_time_median_magnitude)
                    stop_time_median_magnitude = stop_time_median_magnitude.astimezone(pytz.utc)
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id) 
                    tess.median_magnitude=median_magnitude
                    tess.start_time_median_magnitude=start_time_median_magnitude
                    tess.stop_time_median_magnitude=stop_time_median_magnitude
                    tess.bortle_level = scale[np.digitize(median_magnitude, ranges) - 1]
                    tess.save()
                    if device_type==1:
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.median_magnitude = median_magnitude
                    hitorical_values.bortle_level = scale[np.digitize(median_magnitude, ranges) - 1]
                    hitorical_values.save()
                    self.__log.info("End Load historical bortle "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess: "+tess_id)
                    pass

    def historical_stddev_magnitude(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                            from(bucket: \""+self.INFLUX_BUCKET+"\")\
                                  |> range(start: "+from_date+", stop: "+to_date+")\
                                  |> hourSelection(start: 4, stop: 8)\
                                  |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                  |> filter(fn: (r) => r[\"_field\"] == \""+mag+"\")\
                                  |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                                  |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                     moon =\
                            from(bucket: \"iluminaconciencia-luna\")\
                                  |> range(start: "+from_date+", stop: "+to_date+")\
                                  |> hourSelection(start: 4, stop: 8)\
                                  |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                  |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                                  |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                                  |> filter(fn: (r) => r[\"_value_moon\"] == 0)\
                                  |> stddev(column: \"_value_tess\")\
                    "
            result = self.client.query_api().query(query)
            result_json=json.loads(result.to_json())  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    stddev_magnitude=item['_value_tess'] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.stddev_magnitude = stddev_magnitude
                    hitorical_values.save()
                    self.__log.info("End Load historical stddev magnitude "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_median_magnitude_clear(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                        from(bucket: \"iluminaconciencia-tess\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                              |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                              |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                              |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                              |> filter(fn: (r) => r[\"weather\"] == \"Despejado\")\
                    moon =\
                        from(bucket: \"iluminaconciencia-luna\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                    |> filter(fn: (r) => r[\"_value\"] == 0)\
                    |> median(column: \""+mag+"\")\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    median_magnitude_clear=item[mag] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.median_magnitude_clear=median_magnitude_clear
                    hitorical_values.save()
                    self.__log.info("End Load historical median magnitude clear "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_stddev_magnitude_clear(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                        from(bucket: \"iluminaconciencia-tess\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                              |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                              |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                              |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                              |> filter(fn: (r) => r[\"weather\"] == \"Despejado\")\
                    moon =\
                        from(bucket: \"iluminaconciencia-luna\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                    |> filter(fn: (r) => r[\"_value\"] == 0)\
                    |> stddev(column: \""+mag+"\")\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    stddev_magnitude_clear=item[mag] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.stddev_magnitude_clear=stddev_magnitude_clear
                    hitorical_values.save()
                    self.__log.info("End Load historical stddev magnitude clear "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_median_magnitude_covered(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                        from(bucket: \"iluminaconciencia-tess\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                              |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                              |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                              |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                              |> filter(fn: (r) => r[\"weather\"] == \"Cubierto\")\
                    moon =\
                        from(bucket: \"iluminaconciencia-luna\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                    |> filter(fn: (r) => r[\"_value\"] == 0)\
                    |> median(column: \""+mag+"\")\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    median_magnitude_covered=item[mag] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.median_magnitude_covered=median_magnitude_covered
                    hitorical_values.save()
                    self.__log.info("End Load historical median magnitude covered "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_stddev_magnitude_covered(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                        from(bucket: \"iluminaconciencia-tess\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                              |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                              |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                              |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                              |> filter(fn: (r) => r[\"weather\"] == \"Cubierto\")\
                    moon =\
                        from(bucket: \"iluminaconciencia-luna\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                    |> filter(fn: (r) => r[\"_value\"] == 0)\
                    |> stddev(column: \""+mag+"\")\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    stddev_magnitude_covered=item[mag] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.stddev_magnitude_covered=stddev_magnitude_covered
                    hitorical_values.save()
                    self.__log.info("End Load historical median magnitude covered "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_median_magnitude_cloudy(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                        from(bucket: \"iluminaconciencia-tess\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                              |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                              |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                              |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                              |> filter(fn: (r) => r[\"weather\"] == \"Nublado\")\
                    moon =\
                        from(bucket: \"iluminaconciencia-luna\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                    |> filter(fn: (r) => r[\"_value\"] == 0)\
                    |> median(column: \""+mag+"\")\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    median_magnitude_cloudy=item[mag] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.median_magnitude_cloudy=median_magnitude_cloudy
                    hitorical_values.save()
                    self.__log.info("End Load historical median magnitude cloudy "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_stddev_magnitude_cloudy(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "iluminaconciencia =\
                        from(bucket: \"iluminaconciencia-tess\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                              |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                              |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                              |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                              |> filter(fn: (r) => r[\"weather\"] == \"Nublado\")\
                    moon =\
                        from(bucket: \"iluminaconciencia-luna\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> hourSelection(start: 4, stop: 8)\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"_field\"] == \"brightness\")\
                              |> aggregateWindow(every: 10m, fn: median, createEmpty: true)\
                    join(tables: {tess: iluminaconciencia, moon: moon}, on: [\"_time\"], method: \"inner\")\
                    |> filter(fn: (r) => r[\"_value\"] == 0)\
                    |> stddev(column: \""+mag+"\")\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['name'] #'tess_id'
                    stddev_magnitude_cloudy=item[mag] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.stddev_magnitude_cloudy=stddev_magnitude_cloudy
                    hitorical_values.save()
                    self.__log.info("End Load historical median magnitude cloudy "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_total_measurements_month(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "from(bucket: \"iluminaconciencia-tess\")\
                                    |> range(start: "+from_date+", stop: "+to_date+")\
                                    |> hourSelection(start: 4, stop: 8)\
                                    |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                                    |> filter(fn: (r) => r._measurement == \"mqtt_consumer\")\
                                    |> filter(fn: (r) => r[\"_field\"] == \""+mag+"\")\
                                    |> filter(fn: (r) => exists r.influxdb_tag)\
                                    |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                                    |> count(column: \"_value\")\
                                    |> map(fn: (r) => ({ _name: r[\"name\"], _value: r[\"_value\"] }))\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['_name'] #'tess_id'
                    total_measurements_month=item['_value'] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.total_measurements_month=total_measurements_month*10
                    hitorical_values.save()
                    self.__log.info("End Load historical total measurements month "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_percentage_measurements_month(self, from_date, to_date, now, period):
        #se restringe a solo horario de medición de 00 a 05 hrs (5 horas)
        totalMinutes = 5 * 60 * 30
        tessws=HistoricalValuesTessW.objects.filter(day=period.day, month=period.month, year=period.year)
        for tessw in tessws:
            try:
                tessw.percentage_measurements_month=tessw.total_measurements_month/totalMinutes
                tessw.save()
            except Exception as e:
                print(e)
                print("Tess:" +str(tessw.tess))
                pass
        tess4cs=HistoricalValuesTess4C.objects.filter(day=period.day, month=period.month, year=period.year)
        for tess4c in tess4cs:
            try:
                tess4c.percentage_measurements_month=tess4c.total_measurements_month/totalMinutes
                tess4c.save()
            except Exception as e:
                print(e)
                print("Tess:" +str(tess4c.tess))
                pass
        self.__log.info("End Load historical percentage measurements month")

    def historical_total_measurements_month_clear(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "from(bucket: \"iluminaconciencia-tess\")\
                                    |> range(start: "+from_date+", stop: "+to_date+")\
                                    |> hourSelection(start: 4, stop: 8)\
                                    |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                    |> filter(fn: (r) => exists r.influxdb_tag)\
                                    |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                                    |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                                    |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                                    |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                                    |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                                    |> filter(fn: (r) => r[\"weather\"] == \"Despejado\")\
                                    |> count(column: \""+mag+"\")\
                                    |> map(fn: (r) => ({ _name: r[\"name\"], _value: r[\""+mag+"\"] }))\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['_name'] #'tess_id'
                    total_measurements_clear=item['_value'] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.total_measurements_clear=total_measurements_clear*10
                    hitorical_values.save()
                    self.__log.info("End Load historical total measurements month clear nights "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_percentage_measurements_month_clear(self, from_date, to_date, now, period):
        #se restringe a solo horario de medición de 00 a 05 hrs (5 horas)
        tessws=HistoricalValuesTessW.objects.filter(day=period.day, month=period.month, year=period.year)
        for tessw in tessws:
            try:
                tessw.percentage_measurements_clear=tessw.total_measurements_clear/tessw.total_measurements_month
                tessw.save()
            except Exception as e:
                print(e)
                print("Tess:" +str(tessw.tess))
                pass
        tess4cs=HistoricalValuesTess4C.objects.filter(day=period.day, month=period.month, year=period.year)
        for tess4c in tess4cs:
            try:
                tess4c.percentage_measurements_clear=tessw.total_measurements_clear/tess4c.total_measurements_month
                tess4c.save()
            except Exception as e:
                print(e)
                print("Tess:" +str(tess4c.tess))
                pass
        self.__log.info("End Load historical percentage measurements month clear nights")

    def historical_total_measurements_month_covered(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "from(bucket: \"iluminaconciencia-tess\")\
                                    |> range(start: "+from_date+", stop: "+to_date+")\
                                    |> hourSelection(start: 4, stop: 8)\
                                    |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                                    |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                    |> filter(fn: (r) => exists r.influxdb_tag)\
                                    |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                                    |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                                    |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                                    |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                                    |> filter(fn: (r) => r[\"weather\"] == \"Cubierto\")\
                                    |> count(column: \""+mag+"\")\
                                    |> map(fn: (r) => ({ _name: r[\"name\"], _value: r[\""+mag+"\"] }))\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['_name'] #'tess_id'
                    total_measurements_covered=item['_value'] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.total_measurements_covered=total_measurements_covered*10
                    hitorical_values.save()
                    self.__log.info("End Load historical total measurements month covered nights "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_percentage_measurements_month_covered(self, from_date, to_date, now, period):
        #se restringe a solo horario de medición de 00 a 05 hrs (5 horas)
        tessws=HistoricalValuesTessW.objects.filter(day=period.day, month=period.month, year=period.year)
        for tessw in tessws:
            try:
                tessw.percentage_measurements_covered=tessw.total_measurements_covered/tessw.total_measurements_month
                tessw.save()
            except Exception as e:
                print(e)
                print("Tess:" +str(tessw.tess))
                pass
        tess4cs=HistoricalValuesTess4C.objects.filter(day=period.day, month=period.month, year=period.year)
        for tess4c in tess4cs:
            try:
                tess4c.percentage_measurements_covered=tess4c.total_measurements_covered/tess4c.total_measurements_month
                tess4c.save()
            except Exception as e:
                print(e)
                print("Tess:" +str(tess4c.tess))
                pass
        self.__log.info("End Load historical percentage measurements month covered nights")

    def historical_total_measurements_month_cloudy(self, from_date, to_date, now, period):
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for instrument in instruments:
            device_type=instrument.device_type
            if device_type==1:
                mag='mag'
            else:
                mag='F1_mag'
            query = "from(bucket: \"iluminaconciencia-tess\")\
                                    |> range(start: "+from_date+", stop: "+to_date+")\
                                    |> hourSelection(start: 4, stop: 8)\
                                    |> filter(fn: (r) => r[\"name\"] == \""+instrument.id+"\")\
                                    |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                                    |> filter(fn: (r) => exists r.influxdb_tag)\
                                    |> aggregateWindow(every: 10m, fn: median, createEmpty: false)\
                                    |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\
                                    |> map(fn: (r) => ({r with temp: r.tamb-r.tsky}))\
                                    |> map(fn: (r) => ({r with weather: if r.temp < 11.66 then \"Cubierto\" else if  r.temp>= 11.66 and r.temp <= 20 then \"Nublado\" else \"Despejado\" }))\
                                    |> filter(fn: (r) => r[\"weather\"] == \"Nublado\")\
                                    |> count(column: \""+mag+"\")\
                                    |> map(fn: (r) => ({ _name: r[\"name\"], _value: r[\""+mag+"\"] }))\
                    "
            result = self.client.query_api().query(query)  
            if len(result) == 0:
                self.__log.warning(f"Sin datos para instrumento {instrument.id}")
                continue
            result_json=json.loads(result.to_json())
            for item in result_json:
                try:
                    tess_id=item['_name'] #'tess_id'
                    total_measurements_cloudy=item['_value'] #'magnitude'
                    if device_type==1:
                        tess=TessW.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    elif device_type==2:
                        tess=Tess4C.objects.get(id=tess_id)
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year, calculation_date=now)
                    hitorical_values.total_measurements_cloudy=total_measurements_cloudy*10
                    hitorical_values.save()
                    self.__log.info("End Load historical total measurements month cloudy nights "+tess_id)
                except Exception as e:
                    print(e)
                    print("Tess:" +tess_id)
                    pass

    def historical_percentage_measurements_month_cloudy(self, from_date, to_date, now, period):
        #se restringe a solo horario de medición de 00 a 05 hrs (5 horas)
        tessws=HistoricalValuesTessW.objects.filter(day=period.day, month=period.month, year=period.year)
        for tessw in tessws:
            try:
                tessw.percentage_measurements_cloudy=tessw.total_measurements_cloudy/tessw.total_measurements_month
                tessw.save()
            except Exception as e:
                print(e)
                print("Tess:" +str(tessw.tess))
                pass
        tess4cs=HistoricalValuesTess4C.objects.filter(day=period.day, month=period.month, year=period.year)
        for tess4c in tess4cs:
            try:
                tess4c.percentage_measurements_cloudy=tess4c.total_measurements_cloudy/tess4c.total_measurements_month
                tess4c.save()
            except Exception as e:
                print(e)
                print("Tess:" +str(tess4c.tess))
                pass
        self.__log.info("End Load historical percentage measurements month cloudy nights")

    def get_month_data(self, tess, from_date, to_date, now, period):
        device_type=tess.device_type
        if device_type==1:
            mag='mag'
        else:
            mag='F1_mag'
        query = "from(bucket: \"iluminaconciencia-tess\")\
                              |> range(start: "+from_date+", stop: "+to_date+")\
                              |> filter(fn: (r) => r[\"name\"] == \""+tess.id+"\")\
                              |> filter(fn: (r) => r[\"_measurement\"] == \"mqtt_consumer\")\
                              |> filter(fn: (r) => r[\"_field\"] == \"mag\" or r[\"_field\"] == \"tamb\" or r[\"_field\"] == \"tsky\")\
                              |> filter(fn: (r) => exists r.influxdb_tag)\
                              |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)\
                              |> pivot(\
                                  rowKey:[\"_time\"],\
                                  columnKey: [\"_field\"],\
                                  valueColumn: \"_value\"\
                               )\
                "
        url = f"http://{self.INFLUX_URL}/api/v2/query?org={self.INFLUX_ORG}"
        headers = {
            "Authorization": f"Token {self.INFLUX_TOKEN}",
            "Accept": "text/csv",
            "Content-Type": "application/vnd.flux"
        }
        response = requests.post(url, headers=headers, data=query)
        if response.status_code == 200:
            if len(response.text)>4:
                with open(f'{period.day}_{period.month}_{period.year}_{tess.id}.csv', 'w') as file:
                    file.write(response.text)
                    print("CSV download success")
                    return file.name
        else:
            print(f"Error download CSV: {response.status_code} - {response.text}")

    def create_folder_s3(self, s3, s3_folder):
        try:
            # Verify if exists folder
            result = s3.list_objects_v2(Bucket=self.AWS_BUCKET_NAME_DATA, Prefix=s3_folder)
            if 'Contents' in result:
                print(f'Folder s3://{self.AWS_BUCKET_NAME_DATA}/{s3_folder} exist.')
            else:
                # Create folder
                s3.put_object(Bucket=self.AWS_BUCKET_NAME_DATA, Key=(s3_folder + '/'))
                print(f'Folder s3://{self.AWS_BUCKET_NAME_DATA}/{s3_folder} created.')
        except NoCredentialsError:
            print('Error Credencials.')
        except PartialCredentialsError:
            print('Incomplete Credencials.')
        except Exception as e:
            print(f'Error: {e}')

    def upload_month_data_by_date(self, from_date, to_date, now, period):
        self.__log.info("Start Upload Month Values")
        s3 = boto3.client('s3', aws_access_key_id=self.AWS_ACCESS_KEY_ID_DATA, aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY_DATA)
        formatted_date = f"{period.day}_{period.month}_{period.year}"
        base_folder = "tessw_data/by_date"
        s3_folder=f"{base_folder}/{formatted_date}/"
        self.create_folder_s3(s3, s3_folder)
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for tess in instruments:
            try:
                file=self.get_month_data(tess, from_date, to_date, now, period)
                if file:
                    s3_file = os.path.join(s3_folder, file)
                    s3.upload_file(file, self.AWS_BUCKET_NAME_DATA, s3_file)
                    s3.put_object_acl(Bucket=self.AWS_BUCKET_NAME_DATA, Key=s3_file, ACL='public-read')
                    if tess.device_type==1:
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year)
                    elif tess.device_type==2:
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year)
                    region = self.AWS_S3_REGION_NAME
                    url = f"https://{self.AWS_BUCKET_NAME_DATA}.s3.{region}.amazonaws.com/{s3_folder}/{file}"
                    hitorical_values.data_url=url
                    hitorical_values.save()
                    os.remove(file)
            except:
                self.__log.error(f"Error con create csv file {tess}")
        self.__log.info("End Upload Month Values")

    def upload_month_data_by_id(self, from_date, to_date, now, period):
        self.__log.info("Start Upload Month Values")
        s3 = boto3.client('s3', aws_access_key_id=self.AWS_ACCESS_KEY_ID_DATA, aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY_DATA)
        formatted_date = f"{period.day}_{period.month}_{period.year}"
        base_folder = "tessw_data/by_id"
        instruments = list(TessW.objects.all()) + list(Tess4C.objects.all())
        for tess in instruments:
            s3_folder=f"{base_folder}/{tess.id}/"
            self.create_folder_s3(s3, s3_folder)
            try:
                file=self.get_month_data(tess.id, from_date, to_date, now, period)
                if file:
                    s3_file = os.path.join(s3_folder, file)
                    s3.upload_file(file, self.AWS_BUCKET_NAME_DATA, s3_file)
                    s3.put_object_acl(Bucket=self.AWS_BUCKET_NAME_DATA, Key=s3_file, ACL='public-read')
                    if tess.device_type==1:
                        hitorical_values, created = HistoricalValuesTessW.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year)
                    elif tess.device_type==2:
                        hitorical_values, created = HistoricalValuesTess4C.objects.get_or_create(tess=tess, day=period.day, month=period.month, year=period.year)
                    region = self.AWS_S3_REGION_NAME
                    url = f"https://{self.AWS_BUCKET_NAME_DATA}.s3.{region}.amazonaws.com/{s3_folder}/{file}"
                    hitorical_values.data_url=url
                    hitorical_values.save()
                    os.remove(file)
            except:
                self.__log.error(f"Error con create csv file {tess}")
        self.__log.info("End Upload Month Values")

    def __init__(self):
        env = environ.Env()
        environ.Env.read_env()
        self.__log = logging.getLogger(__name__)
        self.__log.setLevel(logging.DEBUG)
        handler = logging.FileHandler(env('LOG_FILENAME'))
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(lineno)d;%(asctime)s;%(levelname)s;%(message)s')
        handler.setFormatter(formatter)
        self.__log.addHandler(handler)
        self.INFLUX_BUCKET = env('INFLUX_BUCKET')
        self.INFLUX_URL = env('INFLUX_URL')
        self.INFLUX_ORG = env('INFLUX_ORG')
        self.INFLUX_TOKEN = env('INFLUX_TOKEN')
        self.AWS_ACCESS_KEY_ID_DATA = env('AWS_ACCESS_KEY_ID_DATA')
        self.AWS_SECRET_ACCESS_KEY_DATA = env('AWS_SECRET_ACCESS_KEY_DATA')
        self.AWS_BUCKET_NAME_DATA = env('AWS_BUCKET_NAME_DATA')
        self.AWS_S3_REGION_NAME = env('AWS_S3_REGION_NAME')
        self.client = InfluxDBClient(url=env('INFLUX_URL'), token=env('INFLUX_TOKEN'), org=env('INFLUX_ORG'), timeout=3000_000)