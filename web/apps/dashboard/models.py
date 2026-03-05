from django.db import models
from .validators import *

DEVICE_STATUS = (
    (0, 'ON'),
    (1, 'OFF'),
    (2, 'FAIL'),
)

DEVICE_TYPE = (
    ('TessW', 'TessW'),
    ('Tess4C', 'Tess4C'),
    ('SQC', 'SQC'),
)

FINANCIAMIENTO = (
    (0, 'FIC-R Región de Coquimbo 2022'),
    (1, 'ESO Comité Mixto-Gobierno de Chile 2021'),
)

class Instrument(models.Model):
    id = models.CharField(max_length=200, primary_key=True, verbose_name='id Nodo', null=False)
    name = models.CharField(max_length=200, blank=False, null=False)
    commune = models.CharField(max_length=200, blank=True, null=True, default='', verbose_name='comuna')
    location = models.CharField(max_length=200, blank=True, null=True, default='', verbose_name='localidad')
    region = models.CharField(max_length=200, blank=True, null=True, default='', verbose_name='región')
    financiamiento = models.IntegerField(choices=FINANCIAMIENTO,blank=True,null=True)
    lat = models.FloatField(blank=True, null=True, verbose_name='latitud')
    lon = models.FloatField(blank=True, null=True, verbose_name='longitud')
    image = models.ImageField(blank=True, null=True, upload_to='tess_location/', height_field=None, width_field=None, max_length=100, validators=[validate_image_tess_location], help_text='Tamano maximo de la imagen es 1Mb, sus dimensiones deben ser de 420x420 pixeles')
    last_update = models.DateTimeField(blank=True, null=True)

    class Meta:
        abstract = True

class TessW(Instrument):
    last_frequency = models.FloatField(blank=True, null=True)
    last_magnitude = models.FloatField(blank=True, null=True)
    last_ambient_temperature = models.FloatField(blank=True, null=True)
    last_sky_temperature = models.FloatField(blank=True, null=True)
    status = models.IntegerField(choices=DEVICE_STATUS,blank=True, null=True,default=0)
    median_magnitude = models.FloatField(blank=True, null=True)
    start_time_median_magnitude = models.DateTimeField(blank=True, null=True)
    stop_time_median_magnitude = models.DateTimeField(blank=True, null=True)
    bortle_level = models.IntegerField(blank=True, null=True)
    active = models.BooleanField(default=True)
    institution_email = models.EmailField(max_length = 254, blank=True, null=True, default='')
    institution_email_verification = models.BooleanField(default=False)
    solar = models.BooleanField(default=False)

    class Meta:
        verbose_name_plural = "Tess-W Nodes"

    def __unicode__(self):
        return self.name

    def __str__(self):
        return "%s" %self.name

    def status_verbose(self):
        return dict(DEVICE_STATUS)[self.status]
    
    def financiamiento_verbose(self):
        return dict(FINANCIAMIENTO)[self.financiamiento]

class Tess4C(Instrument):
    last_frequency = models.FloatField(blank=True, null=True)
    last_magnitude = models.FloatField(blank=True, null=True)
    last_frequency_f2 = models.FloatField(blank=True, null=True)
    last_magnitude_f2 = models.FloatField(blank=True, null=True)
    last_frequency_f3 = models.FloatField(blank=True, null=True)
    last_magnitude_f3 = models.FloatField(blank=True, null=True)
    last_frequency_f4 = models.FloatField(blank=True, null=True)
    last_magnitude_f4 = models.FloatField(blank=True, null=True)
    last_ambient_temperature = models.FloatField(blank=True, null=True)
    last_sky_temperature = models.FloatField(blank=True, null=True)
    status = models.IntegerField(choices=DEVICE_STATUS,blank=True, null=True,default=0)
    median_magnitude = models.FloatField(blank=True, null=True)
    start_time_median_magnitude = models.DateTimeField(blank=True, null=True)
    stop_time_median_magnitude = models.DateTimeField(blank=True, null=True)
    bortle_level = models.IntegerField(blank=True, null=True)
    active = models.BooleanField(default=True)
    institution_email = models.EmailField(max_length = 254, blank=True, null=True, default='')
    institution_email_verification = models.BooleanField(default=False)
    solar = models.BooleanField(default=False)

    class Meta:
        verbose_name_plural = "Tess4C Nodes"

    def __unicode__(self):
        return self.name

    def __str__(self):
        return "%s" %self.name

    def status_verbose(self):
        return dict(DEVICE_STATUS)[self.status]
    
    def financiamiento_verbose(self):
        return dict(FINANCIAMIENTO)[self.financiamiento]

class SQC(Instrument):
    bortle_level = models.IntegerField(blank=True, null=True)
    zenith_magnitude = models.FloatField(blank=True, null=True)

    class Meta:
        verbose_name_plural = "SQCs"

class Moon(models.Model):
    timestamp = models.DateTimeField(blank=True, null=True)
    brightness = models.FloatField(blank=True, null=True)

class LastNight(models.Model):
    record_time = models.DateTimeField(blank=True, null=True)
    ambient_temperature = models.FloatField(blank=True, null=True)
    magnitude = models.FloatField(blank=True, null=True)
    sky_temperature = models.FloatField(blank=True, null=True) 
    weather = models.CharField(max_length=100, blank=True, null=True)

class LastNightTessW(LastNight):
    tess = models.ForeignKey(TessW, on_delete=models.CASCADE)

class LastNightTess4C(LastNight):
    tess = models.ForeignKey(Tess4C, on_delete=models.CASCADE)

class LastWeek(models.Model):
    record_time = models.DateTimeField(blank=True, null=True)
    ambient_temperature = models.FloatField(blank=True, null=True)
    magnitude = models.FloatField(blank=True, null=True)
    sky_temperature = models.FloatField(blank=True, null=True) 
    weather = models.CharField(max_length=100, blank=True, null=True)

class LastWeekTessW(LastWeek):
    tess = models.ForeignKey(TessW, on_delete=models.CASCADE)

class LastWeekTess4C(LastWeek):
    tess = models.ForeignKey(Tess4C, on_delete=models.CASCADE)

class LastMonth(models.Model):
    record_time = models.DateTimeField(blank=True, null=True)
    ambient_temperature = models.FloatField(blank=True, null=True)
    magnitude = models.FloatField(blank=True, null=True)
    sky_temperature = models.FloatField(blank=True, null=True) 
    weather = models.CharField(max_length=100, blank=True, null=True)

class LastMonthTessW(LastMonth):
    tess = models.ForeignKey(TessW, on_delete=models.CASCADE)

class LastMonthTess4C(LastMonth):
    tess = models.ForeignKey(Tess4C, on_delete=models.CASCADE)

class HistoricalValues(models.Model):
    day = models.IntegerField(blank=False, null=False)
    month = models.IntegerField(blank=False, null=False)
    year = models.IntegerField(blank=False, null=False)
    calculation_datetime = models.DateTimeField(blank=True, null=True)
    calculation_date = models.DateField(blank=False, null=False)
    median_magnitude = models.FloatField(blank=True, null=True)
    median_magnitude_clear = models.FloatField(blank=True, null=True)    
    median_magnitude_covered = models.FloatField(blank=True, null=True)
    median_magnitude_cloudy = models.FloatField(blank=True, null=True)
    stddev_magnitude = models.FloatField(blank=True, null=True)
    stddev_magnitude_clear = models.FloatField(blank=True, null=True)    
    stddev_magnitude_covered = models.FloatField(blank=True, null=True)
    stddev_magnitude_cloudy = models.FloatField(blank=True, null=True)
    total_measurements_month = models.IntegerField(blank=True, null=True)
    total_measurements_clear = models.IntegerField(blank=True, null=True)
    total_measurements_covered = models.IntegerField(blank=True, null=True)
    total_measurements_cloudy = models.IntegerField(blank=True, null=True)
    percentage_measurements_month = models.FloatField(blank=True, null=True)
    percentage_measurements_clear = models.FloatField(blank=True, null=True)
    percentage_measurements_covered = models.FloatField(blank=True, null=True)
    percentage_measurements_cloudy = models.FloatField(blank=True, null=True)
    bortle_level = models.IntegerField(blank=True, null=True)
    data_url = models.CharField(max_length=1000, blank=True, null=True, default='')

    class Meta:
        verbose_name_plural = "Historical Values"

class HistoricalValuesTessW(HistoricalValues):
    tess = models.ForeignKey(TessW, on_delete=models.CASCADE)

class HistoricalValuesTess4C(HistoricalValues):
    tess = models.ForeignKey(Tess4C, on_delete=models.CASCADE)