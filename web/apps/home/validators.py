# -*- coding: utf-8 -*-
from django.core.files.images import get_image_dimensions
from django.core.exceptions import ValidationError
import os.path

def validate_image_tess_location(fieldfile_obj):
    filesize = fieldfile_obj.file.size
    megabyte_limit = 1.0
    if filesize > megabyte_limit*1024*1024:
        raise ValidationError("El tamano maximo permitido es de %sMB" % str(megabyte_limit))
    w, h = get_image_dimensions(fieldfile_obj)
    if w != 420 or h != 420:
        raise ValidationError("Las dimensiones de la foto son de %ix%i, y estas deben ser de 420x420 pixeles" %(w,h))

def validate_image_noticia(fieldfile_obj):
    filesize = fieldfile_obj.file.size
    w, h = get_image_dimensions(fieldfile_obj)
    megabyte_limit = 1.0
    if filesize > megabyte_limit*1024*1024:
        raise ValidationError("El tamano maximo permitido es de %sMB" % str(megabyte_limit))
    if w != 370 or h != 240:
        raise ValidationError("Las dimensiones de la imagen son de %ix%i pixeles, y estas deben ser de 370x240px" %(w,h))
