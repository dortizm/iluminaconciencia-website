from rest_framework import generics, status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.dashboard.models import Tess4C, TessW

from .serializers import Tess4CCoordinatesSerializer, TessWCoordinatesSerializer


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
    """Lista conjunta de TESS-W y TESS-4C.

    Se puede filtrar mediante ``?type=tess-w`` o ``?type=tess-4c``.
    """

    valid_types = {"tess-w", "tess-4c"}

    def get(self, request):
        instrument_type = request.query_params.get("type")

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

        if instrument_type in (None, "tess-w"):
            instruments.extend(
                TessWCoordinatesSerializer(
                    TessW.objects.all().order_by("name", "id"), many=True
                ).data
            )

        if instrument_type in (None, "tess-4c"):
            instruments.extend(
                Tess4CCoordinatesSerializer(
                    Tess4C.objects.all().order_by("name", "id"), many=True
                ).data
            )

        instruments.sort(key=lambda item: (item["name"].casefold(), item["id"]))
        return Response(instruments)
