from rest_framework import serializers
from .models import TessW

class TessWSerializer(serializers.ModelSerializer):
    financiamiento_display = serializers.CharField(source='financiamiento_verbose', read_only=True)
    status_display = serializers.CharField(source='status_verbose', read_only=True)

    class Meta:
        model = TessW
        fields = [
            'id', 'name', 'commune', 'location', 'region',
            'financiamiento', 'financiamiento_display',
            'lat', 'lon', 'image', 'last_update', 'last_frequency',
            'last_magnitude', 'last_ambient_temperature', 'last_sky_temperature',
            'status', 'status_display', 'median_magnitude',
            'start_time_median_magnitude', 'stop_time_median_magnitude',
            'bortle_level', 'active', 'institution_email',
            'institution_email_verification', 'solar'
        ]
