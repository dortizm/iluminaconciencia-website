from django.urls import path

from .views import (
    InstrumentCoordinatesList,
    Tess4CCoordinatesList,
    TessWCoordinatesList,
)

app_name = "api"

urlpatterns = [
    path("instruments/", InstrumentCoordinatesList.as_view(), name="instrument-list"),
    path("tess-w/", TessWCoordinatesList.as_view(), name="tess-w-list"),
    path("tess-4c/", Tess4CCoordinatesList.as_view(), name="tess-4c-list"),
]
