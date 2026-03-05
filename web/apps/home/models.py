from django.db import models
from .validators import *
from ckeditor_uploader.fields import RichTextUploadingField
from django.dispatch import receiver
from django.db.models.signals import post_delete, pre_save, post_save
from PIL import Image
from io import BytesIO
from django.core.files.uploadedfile import InMemoryUploadedFile

class Multimedia(models.Model):
    titulo = models.CharField(max_length=200, blank=False, null=False)
    descripcion = models.CharField(max_length=1500, blank=True, null=True)
    linkcode = models.CharField(max_length=100, blank=True, null=True, help_text='Código de Youtube')
    imagen = models.ImageField(upload_to='media/multimedia/', height_field=None, width_field=None, max_length=1980, blank=True, null=True, help_text='Ancho máximo de la images en 1980 pixeles')
    mostrar = models.BooleanField(default=True, help_text="Visualizar este multimedia.")
    orden = models.IntegerField(help_text="Orden en el que se publican estos contenidos multimedia.")

    def __unicode__(self):
        return self.titulo

    def __str__(self):
        return "%s" %self.titulo

class Noticia(models.Model):
    fecha = models.DateTimeField()
    titulo = models.CharField(max_length=300)
    resumen = models.CharField(max_length=500)
    cuerpo = RichTextUploadingField(max_length=15000, blank=False, null=False, config_name='awesome_ckeditor')
    imagen = models.ImageField(upload_to='media/noticias/', height_field=None, width_field=None, max_length=100, validators=[validate_image_noticia], help_text='Tamano maximo de la imagen es de 1Mb y sus dimensiones deben ser de 370x240px')
    mostrar = models.BooleanField(default=True, help_text="Visualizar Noticia.")
    mostrar_principal = models.BooleanField(default=False, help_text="Visualizar actividad en index del sitio.")

    class Meta:
        ordering = ['-fecha']
        verbose_name_plural = "Noticias"

    def __unicode__(self):
        return self.titulo

    def __str__(self):
        return "%s" %self.titulo

    def image_preview(self):
        if self.imagen:
            return u'<img width="370px" height="auto" src="%s" />' % self.imagen.url
        else:
            return '(Sin imagen)'
    image_preview.short_description = 'Imagen'
    image_preview.allow_tags = True


# @receiver(post_delete, sender=Noticia)
# def auto_delete_file_on_delete_noticia(sender, instance, **kwargs):
#     if instance.imagen:
#         if os.path.isfile(instance.imagen.path):
#             os.remove(instance.imagen.path)


# @receiver(pre_save, sender=Noticia)
# def auto_delete_file_on_change_noticia(sender, instance, **kwargs):
#     if not instance.pk:
#         return False
#     try:
#         old_imagen = Noticia.objects.get(pk=instance.pk).imagen
#     except Noticia.DoesNotExist:
#         return False
#     new_imagen = instance.imagen
#     if old_imagen and not old_imagen == new_imagen:
#         if os.path.isfile(old_imagen.path):
#             os.remove(old_imagen.path)


class Solicitud(models.Model):
    nombre = models.CharField(max_length=50)
    apellidos = models.CharField(max_length=50)
    institucion = models.CharField(max_length=100)
    correo_electronico = models.EmailField(unique=True,default=None)
    archivo_csv_luminarias = models.FileField(upload_to='media/luminarias/',default=None)
    archivo_imagen_pago = models.FileField(upload_to='media/pagos/',default=None)
    fecha_solicitud = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'{self.nombre} {self.apellidos}'
    

class Galeria(models.Model):
    nombre = models.CharField(max_length=255)
    autor = models.CharField(max_length=255)
    indice = models.IntegerField()
    imagen_hd = models.ImageField(upload_to='media/fotos_hd/')
    imagen_thumb = models.ImageField(upload_to='media/fotos_thumb/', blank=True, null=True)
    mostrar = models.BooleanField(default=True, help_text="Visualizar Imagen.")
    mostrar_principal = models.BooleanField(default=False, help_text="Visualizar imagen en index del sitio.")

    def save(self, *args, **kwargs):
        if self.imagen_hd:
            img = Image.open(self.imagen_hd)
            img.thumbnail((300, 300))  # tamaño del thumbnail
            output = BytesIO()
            img.save(output, format='JPEG', quality=90)
            output.seek(0)
            self.imagen_thumb = InMemoryUploadedFile(output, 'ImageField', f"{self.nombre}_thumb.jpg", 'image/jpeg', len(output.getvalue()), None)
        super(Galeria, self).save(*args, **kwargs)

    def __str__(self):
        return self.nombre