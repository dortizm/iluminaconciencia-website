from rest_framework import serializers

from apps.dashboard.models import Tess4C, TessW
from apps.home.models import Noticia, Galeria, Multimedia, Articulo

class InstrumentCoordinatesSerializer(serializers.ModelSerializer):
    """Campos públicos comunes a los instrumentos TESS."""

    type = serializers.SerializerMethodField()

    type_name = ""

    def get_type(self, obj):
        return self.type_name

    class Meta:
        fields = ("id", "name", "type", "lat", "lon")
        read_only_fields = fields


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

