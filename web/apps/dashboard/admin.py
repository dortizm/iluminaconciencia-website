from django.contrib import admin
from import_export.admin import ImportExportModelAdmin
from .models import TessW, Tess4C, SQC, Moon, \
LastNightTessW, LastWeekTessW, LastMonthTessW, HistoricalValuesTessW, \
LastNightTess4C, LastWeekTess4C, LastMonthTess4C, HistoricalValuesTess4C 

# Register your models here.
class TessWAdmin(ImportExportModelAdmin):
    list_display = ('id','name', 'commune', 'location', 'financiamiento', 'status', 'last_update' ,'median_magnitude', 'bortle_level', 'active')

class Tess4CAdmin(ImportExportModelAdmin):
    list_display = ('id','name', 'commune', 'location', 'financiamiento', 'status', 'last_update' ,'median_magnitude', 'bortle_level', 'active')

class SQCAdmin(ImportExportModelAdmin):
    list_display = ('id','name', 'commune', 'location', 'financiamiento', 'last_update', 'bortle_level')

class LastNightTessWAdmin(ImportExportModelAdmin):
    list_display = ('tess','record_time', 'ambient_temperature','magnitude','sky_temperature','weather')

class LastWeekTessWAdmin(ImportExportModelAdmin):
    list_display = ('tess','record_time', 'ambient_temperature' ,'magnitude','sky_temperature','weather')

class LastMonthTessWAdmin(ImportExportModelAdmin):
    list_display = ('tess','record_time', 'ambient_temperature','magnitude','sky_temperature','weather')

class LastNightTess4CAdmin(ImportExportModelAdmin):
    list_display = ('tess','record_time', 'ambient_temperature','magnitude','sky_temperature','weather')

class LastWeekTess4CAdmin(ImportExportModelAdmin):
    list_display = ('tess','record_time', 'ambient_temperature' ,'magnitude','sky_temperature','weather')

class LastMonthTess4CAdmin(ImportExportModelAdmin):
    list_display = ('tess','record_time', 'ambient_temperature','magnitude','sky_temperature','weather')

class MoonAdmin(ImportExportModelAdmin):
	list_display = ('timestamp','brightness')

class HistoricalValuesAdmin(ImportExportModelAdmin):
    list_display = ('tess', 'calculation_date', 'month', 'year', 'bortle_level', 'median_magnitude','percentage_measurements_month')

class HistoricalValuesTessWAdmin(ImportExportModelAdmin):
    list_display = ('tess', 'calculation_date', 'month', 'year', 'bortle_level', 'median_magnitude','percentage_measurements_month')

class HistoricalValuesTess4CAdmin(ImportExportModelAdmin):
    list_display = ('tess', 'calculation_date', 'month', 'year', 'bortle_level', 'median_magnitude','percentage_measurements_month')

admin.site.register(TessW, TessWAdmin)
admin.site.register(Tess4C, Tess4CAdmin)
admin.site.register(SQC, SQCAdmin)
admin.site.register(LastNightTessW, LastNightTessWAdmin)
admin.site.register(LastWeekTessW, LastWeekTessWAdmin)
admin.site.register(LastMonthTessW, LastMonthTessWAdmin)
admin.site.register(LastNightTess4C, LastNightTess4CAdmin)
admin.site.register(LastWeekTess4C, LastWeekTess4CAdmin)
admin.site.register(LastMonthTess4C, LastMonthTess4CAdmin)
admin.site.register(Moon, MoonAdmin)
admin.site.register(HistoricalValuesTessW, HistoricalValuesTessWAdmin)
admin.site.register(HistoricalValuesTess4C, HistoricalValuesTess4CAdmin)
