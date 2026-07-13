from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from apps.dashboard.models import Tess4C, TessW


class InstrumentCoordinatesApiTests(APITestCase):
    @classmethod
    def setUpTestData(cls):
        cls.tess_w = TessW.objects.create(
            id="TESS-W-001",
            name="Nodo Alfa",
            lat=-29.9045,
            lon=-71.2489,
        )
        cls.tess_4c = Tess4C.objects.create(
            id="TESS-4C-001",
            name="Nodo Beta",
            lat=-30.0312,
            lon=-70.7081,
        )

    def test_combined_endpoint_returns_both_instrument_types(self):
        response = self.client.get(reverse("api:instrument-list"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(
            {item["type"] for item in response.data}, {"tess-w", "tess-4c"}
        )
        self.assertEqual(
            set(response.data[0]), {"id", "name", "type", "lat", "lon"}
        )

    def test_combined_endpoint_can_filter_tess_w(self):
        response = self.client.get(
            reverse("api:instrument-list"), {"type": "tess-w"}
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], self.tess_w.id)
        self.assertEqual(response.data[0]["type"], "tess-w")

    def test_combined_endpoint_rejects_unknown_type(self):
        response = self.client.get(
            reverse("api:instrument-list"), {"type": "unknown"}
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_specific_endpoints_return_the_expected_model(self):
        tess_w_response = self.client.get(reverse("api:tess-w-list"))
        tess_4c_response = self.client.get(reverse("api:tess-4c-list"))

        self.assertEqual(tess_w_response.status_code, status.HTTP_200_OK)
        self.assertEqual(tess_4c_response.status_code, status.HTTP_200_OK)
        self.assertEqual(tess_w_response.data[0]["type"], "tess-w")
        self.assertEqual(tess_4c_response.data[0]["type"], "tess-4c")
