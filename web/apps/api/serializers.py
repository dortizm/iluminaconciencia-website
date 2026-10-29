from rest_framework import serializers

from apps.dashboard.models import Tess4C, TessW
from apps.home.models import Noticia, Galeria, Multimedia, Articulo

COMMON_FIELDS = [
    'id',
    'name',
    'commune',
    'location',
    'region',
    'financiamiento',
    'financiamiento_display',
    'lat',
    'lon',
    'image',
    'last_update',
    'device_type',
    'last_frequency',
    'last_magnitude',
    'last_ambient_temperature',
    'last_sky_temperature',
    'status',
    'status_display',
    'median_magnitude',
    'start_time_median_magnitude',
    'stop_time_median_magnitude',
    'bortle_level',
    'active',
    'institution_email',
    'institution_email_verification',
    'solar',
    'type',
]

class InstrumentCoordinatesSerializer(serializers.ModelSerializer):
    """Campos públicos comunes a los instrumentos TESS."""

    financiamiento_display = serializers.CharField(
        source='financiamiento_verbose',
        read_only=True
    )

    status_display = serializers.CharField(
        source='status_verbose',
        read_only=True
    )

    type = serializers.SerializerMethodField()

    def get_type(self, obj):
        if isinstance(obj, Tess4C):
            return 'TESS4C'
        return 'TESS-W'

    class Meta:
        fields = COMMON_FIELDS


class TessWCoordinatesSerializer(InstrumentCoordinatesSerializer):
    type_name = "tess-w"

    class Meta(InstrumentCoordinatesSerializer.Meta):
        model = TessW


class Tess4CCoordinatesSerializer(InstrumentCoordinatesSerializer):
    type_name = "tess-4c"

    class Meta(InstrumentCoordinatesSerializer.Meta):
        model = Tess4C

class NoticiasSerializer(serializers.ModelSerializer):
    class Meta:
        model = Noticia
        fields = ("id", "titulo", "resumen", "fecha", "cuerpo", "imagen", "mostrar", "mostrar_principal")

class ArticulosSerializer(serializers.ModelSerializer):
    class Meta:
        model = Articulo
        fields = ("id", "titulo", "fecha", "archivo")

class MultimediasSerializer(serializers.ModelSerializer):
    class Meta:
        model = Multimedia
        fields = ("id", "titulo", "descripcion", "linkcode", "imagen", "mostrar", "orden")

class GaleriasSerializer(serializers.ModelSerializer):
    class Meta:
        model = Galeria
        fields = ("id", "nombre", "autor", "indice", "imagen_hd", "imagen_thumb", "mostrar", "mostrar_principal")

