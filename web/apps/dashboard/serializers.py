from rest_framework import serializers
from .models import TessW, Tess4C


# Campos comunes a ambos tipos de fotómetros
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
    'instrument_type',
]


class BaseTessSerializer(serializers.ModelSerializer):

    financiamiento_display = serializers.CharField(
        source='financiamiento_verbose',
        read_only=True
    )

    status_display = serializers.CharField(
        source='status_verbose',
        read_only=True
    )

    instrument_type = serializers.SerializerMethodField()

    def get_instrument_type(self, obj):
        if isinstance(obj, Tess4C):
            return 'TESS4C'
        return 'TESS-W'

    class Meta:
        fields = COMMON_FIELDS


class TessWSerializer(BaseTessSerializer):

    class Meta(BaseTessSerializer.Meta):
        model = TessW


class Tess4CSerializer(BaseTessSerializer):

    class Meta(BaseTessSerializer.Meta):
        model = Tess4C

        fields = COMMON_FIELDS + [
            'last_frequency_f2',
            'last_magnitude_f2',
            'last_frequency_f3',
            'last_magnitude_f3',
            'last_frequency_f4',
            'last_magnitude_f4',
        ]
