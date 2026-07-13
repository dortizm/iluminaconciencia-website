from rest_framework import serializers

from apps.dashboard.models import Tess4C, TessW


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
