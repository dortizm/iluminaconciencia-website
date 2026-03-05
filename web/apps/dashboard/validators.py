# -*- coding: utf-8 -*-
from django.core.files.images import get_image_dimensions
from django.core.exceptions import ValidationError
import os.path

def validate_image_tess_location(fieldfile_obj):
    filesize = fieldfile_obj.file.size
    megabyte_limit = 4.0
    if filesize > megabyte_limit*1024*1024:
        raise ValidationError("El tamano maximo permitido es de %sMB" % str(megabyte_limit))
    w, h = get_image_dimensions(fieldfile_obj)
    if w != 1413 or h != 1250:
        raise ValidationError("Las dimensiones de la foto son de %ix%i, y estas deben ser de 1200x1200 pixeles" %(w,h))