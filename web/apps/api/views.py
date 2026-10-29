from rest_framework import generics, status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.dashboard.models import Tess4C, TessW
from apps.home.models import Noticia, Articulo, Multimedia, Galeria
from .serializers import Tess4CCoordinatesSerializer, TessWCoordinatesSerializer, NoticiasSerializer, \
ArticulosSerializer, MultimediasSerializer, GaleriasSerializer


class PublicCoordinatesMixin:
    """Configuración común para endpoints públicos de solo lectura."""

    permission_classes = (AllowAny,)
    pagination_class = None


class TessWCoordinatesList(PublicCoordinatesMixin, generics.ListAPIView):
    serializer_class = TessWCoordinatesSerializer
    queryset = TessW.objects.all().order_by("name", "id")


class Tess4CCoordinatesList(PublicCoordinatesMixin, generics.ListAPIView):
    serializer_class = Tess4CCoordinatesSerializer
    queryset = Tess4C.objects.all().order_by("name", "id")


class InstrumentCoordinatesList(PublicCoordinatesMixin, APIView):
    """
    Lista conjunta de TESS-W y TESS-4C.

    Filtros disponibles:
    - ?type=tess-w
    - ?type=tess-4c
    - ?id=stars1496
    - ?id=stars1496&type=tess-4c
    """

    valid_types = {"tess-w", "tess-4c"}

    def get(self, request):
        instrument_type = request.query_params.get("type")
        instrument_id = request.query_params.get("id")

        # Validar tipo de instrumento
        if instrument_type and instrument_type not in self.valid_types:
            return Response(
                {
                    "detail": (
                        "Tipo de instrumento inválido. "
                        "Los valores permitidos son: tess-w, tess-4c."
                    )
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        instruments = []

        # Consultar TESS-W
        if instrument_type in (None, "tess-w"):
            queryset = TessW.objects.all()

            if instrument_id:
                queryset = queryset.filter(id=instrument_id)

            instruments.extend(
                TessWCoordinatesSerializer(
                    queryset.order_by("name", "id"),
                    many=True,
                    context={"request": request}
                ).data
            )

        # Consultar TESS-4C
        if instrument_type in (None, "tess-4c"):
            queryset = Tess4C.objects.all()

            if instrument_id:
                queryset = queryset.filter(id=instrument_id)

            instruments.extend(
                Tess4CCoordinatesSerializer(
                    queryset.order_by("name", "id"),
                    many=True,
                    context={"request": request}
                ).data
            )

        # Ordenar instrumentos por nombre e ID
        instruments.sort(
            key=lambda item: (
                (item.get("name") or "").casefold(),
                item["id"]
            )
        )

        # Si se consulta un ID específico y no existe
        if instrument_id and not instruments:
            return Response(
                {"detail": "Instrumento no encontrado."},
                status=status.HTTP_404_NOT_FOUND
            )

        return Response(instruments)


class NoticiasList(generics.ListAPIView):
    serializer_class = NoticiasSerializer
    queryset = Noticia.objects.all()

class ArticulosList(generics.ListAPIView):
    serializer_class = ArticulosSerializer
    queryset = Articulo.objects.all()

class MultimediasList(generics.ListAPIView):
    serializer_class = MultimediasSerializer
    queryset = Multimedia.objects.all()

class GaleriasList(generics.ListAPIView):
    serializer_class = GaleriasSerializer
    queryset = Galeria.objects.all()