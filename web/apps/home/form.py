from django import forms
from .models import Solicitud
from django.core.exceptions import ValidationError
from django_recaptcha.fields import ReCaptchaField
from django_recaptcha.widgets import ReCaptchaV3

class SolicitudForm(forms.ModelForm):
    recaptcha = ReCaptchaField(widget=ReCaptchaV3)
    class Meta:
        model = Solicitud
        fields = ('nombre', 'apellidos', 'institucion', 'correo_electronico', 'confirmacion_correo', 'archivo_csv_luminarias', 'archivo_imagen_pago','recaptcha')
    nombre = forms.CharField(label='Nombre', max_length=50)
    apellidos = forms.CharField(label='Apellidos', max_length=50)
    institucion = forms.CharField(label='Institución', max_length=100)
    correo_electronico = forms.EmailField(label='Correo Electrónico')
    confirmacion_correo = forms.EmailField(label='Confirmación de Correo')
    archivo_csv_luminarias = forms.FileField(label='Archivo CSV de Luminarias')
    archivo_imagen_pago = forms.FileField(label='Archivo de Imagen del Pago')

    def clean(self):
        cleaned_data = super().clean()
        correo_electronico = cleaned_data.get('correo_electronico')
        confirmacion_correo = cleaned_data.get('confirmacion_correo')
        if correo_electronico != confirmacion_correo:
            raise forms.ValidationError('Los correos electrónicos no coinciden')
        return cleaned_data
    def validate_csv_file(file):
        if file.name.endswith('.csv'):
            return file
        raise ValidationError('Sólo archivos csv son admitidos..')

    def validate_image_file(file):
        if file.name.endswith(('.jpg', '.png', '.jpeg')):
            return file
        raise ValidationError('Sólo archivos en formato JPG, PNG, and JPEG son admitidos..')
