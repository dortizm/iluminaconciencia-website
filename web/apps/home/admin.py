from django.contrib import admin
from .models import Noticia, Multimedia, Galeria, Solicitud

class NoticiaAdmin(admin.ModelAdmin):
    list_display = ('titulo','fecha','resumen', 'mostrar')

class MultimediaAdmin(admin.ModelAdmin):
    list_display = ('titulo','linkcode', 'mostrar', 'orden')

class GaleriaAdmin(admin.ModelAdmin):
    fields = ('nombre', 'autor', 'indice', 'imagen_hd','mostrar','mostrar_principal')

class SolicitudAdmin(admin.ModelAdmin):
    fields = ('nombre', 'apellidos', 'institucion', 'correo_electronico','archivo_csv_luminarias','archivo_imagen_pago')


admin.site.register(Noticia, NoticiaAdmin)
admin.site.register(Multimedia, MultimediaAdmin)
admin.site.register(Galeria, GaleriaAdmin)
admin.site.register(Solicitud, SolicitudAdmin)