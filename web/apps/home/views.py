from django.shortcuts import render
from .models import Multimedia, Noticia, Galeria
from apps.dashboard.models import TessW, Tess4C, SQC, HistoricalValues, HistoricalValuesTessW
from datetime import datetime, timedelta
from .form import SolicitudForm
from django.core.files.base import ContentFile
import environ
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from django.template.loader import render_to_string
from django.conf import settings
from django.db.models import Q

def index(request):
    tessw_nodes = TessW.objects.filter(Q(active=True) | Q(solar=True))
    tess4c_nodes = Tess4C.objects.filter(Q(active=True) | Q(solar=True))
    sqc_nodes = SQC.objects.all()
    news = Noticia.objects.filter(mostrar_principal=True).order_by('-fecha')[:10]
    fotos = Galeria.objects.filter(mostrar_principal=True).order_by('-indice')[:10]
    today = datetime.now()
    last_month = today.replace(day=1) - timedelta(days=1)
    historical = HistoricalValuesTessW.objects.filter(month=today.month,year=today.year).order_by('-percentage_measurements_month')
    period = last_month.strftime("%b %Y")

    return render(request, 'index.html', {'tessw_nodes':tessw_nodes, 'tess4c_nodes':tess4c_nodes, 'sqc_nodes':sqc_nodes, 'news':news,'historical':historical, 'period':period, 'fotos':fotos })

def team(request):
    return render(request, 'team.html')

def multimedia(request):
    multimedias = Multimedia.objects.all().order_by('orden')
    return render(request, 'multimedia.html', {'multimedias':multimedias})

def news_item(request, id):
  try:
    news_item = Noticia.objects.get(id=id)
  except Noticia.DoesNotExist:
    news_item = None
  if news_item:
    return render(request, 'news_item.html', {'news_item': news_item})
  return redirect('/')

def news_list(request):
  try:
    news_list = Noticia.objects.all()
  except Noticia.DoesNotExist:
    news_list = None
  if news_list:
    return render(request, 'news_list.html', {'news_list': news_list})
  return redirect('/')

def galeria_fotos(request):
  try:
    galeria = Galeria.objects.all()
  except galeria.DoesNotExist:
    galeria = None
  if galeria:
    return render(request, 'galeria_fotos.html', {'fotos': galeria})
  return redirect('/')

def solicitar_informe(request):
    if request.method == 'POST':
        form = SolicitudForm(request.POST, request.FILES)
        if form.is_valid():
            solicitud = form.save(commit=False)
            # Renombrar el archivo CSV
            if request.FILES.get('archivo_csv_luminarias'):
                archivo_csv = request.FILES['archivo_csv_luminarias']
                nuevo_nombre_csv = solicitud.nombre+'_'+solicitud.apellidos+'_'+solicitud.institucion+'_luminarias.csv'
                archivo_csv = ContentFile(archivo_csv.read(), nuevo_nombre_csv)
                solicitud.archivo_csv_luminarias.save(nuevo_nombre_csv, archivo_csv)
            # Renombrar el archivo de imagen
            if request.FILES.get('archivo_imagen_pago'):
                archivo_imagen = request.FILES['archivo_imagen_pago']
                nuevo_nombre_imagen = solicitud.nombre+'_'+solicitud.apellidos+'_'+solicitud.institucion+'_pago.jpg'
                archivo_imagen = ContentFile(archivo_imagen.read(), nuevo_nombre_imagen)
                solicitud.archivo_imagen_pago.save(nuevo_nombre_imagen, archivo_imagen)
            solicitud = form.save()
            enviar_correo(solicitud)
            return render(request, 'solicitud_enviada.html')
    else:
        form = SolicitudForm()
    return render(request, 'solicitud_informe.html', {'form': form, 'site_key':settings.RECAPTCHA_PUBLIC_KEY})

def enviar_correo(solicitud):
  env = environ.Env()
  environ.Env.read_env()
  SMTP_SERVER = env('SMTP_SERVER')
  SMTP_USERNAME = env('SMTP_USERNAME')
  SMTP_PASSWORD = env('SMTP_PASSWORD')
  SMTP_SENDER = env('SMTP_SENDER')
  SMTP_RECIPIENT = env('SMTP_RECIPIENT') 
  SMTP_PORT = env('SMTP_PORT')
  # Obtener las URLs de los archivos subidos a S3
  archivo_csv_url = solicitud.archivo_csv_luminarias.url if solicitud.archivo_csv_luminarias else 'No subido'
  archivo_imagen_url = solicitud.archivo_imagen_pago.url if solicitud.archivo_imagen_pago else 'No subido'

  # Crear el cuerpo del correo con los datos del formulario
  mensaje = render_to_string('correo_solicitud.html', {
      'nombre': solicitud.nombre,
      'apellido': solicitud.apellidos,
      'correo': solicitud.correo_electronico,
      'institución:': solicitud.institucion,
      'archivo_csv_url': archivo_csv_url,
      'archivo_imagen_url': archivo_imagen_url,
  })

  body_msg_equipo = MIMEText(mensaje, 'html')
  msg = MIMEMultipart('alternative')
  msg.attach(body_msg_equipo)
  msg['Subject'] = 'Se ha ingresado una solicitud de informe de contaminación lumínica.'
  try:
      server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
      server.ehlo()
      server.starttls()
      server.ehlo()
      server.login(SMTP_USERNAME, SMTP_PASSWORD)
      server.sendmail(SMTP_SENDER,SMTP_RECIPIENT, msg.as_string())
      server.close()
      print("Correo enviado con éxito!")
  except Exception as e:
      print("Error al enviar el correo electrónico:", e)
  print(mensaje)
