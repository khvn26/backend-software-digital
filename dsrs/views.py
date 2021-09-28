from typing import TYPE_CHECKING

from rest_framework import generics, mixins, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import ParseError
from rest_framework.response import Response

from digital.parsers import GzipFileUploadParser
from dsrs import mappers, models, serializers, services

if TYPE_CHECKING:
    from rest_framework.request import Request  # pragma: no cover


class DSRViewSet(
    mixins.RetrieveModelMixin, mixins.ListModelMixin, viewsets.GenericViewSet
):
    queryset = models.DSR.objects.all()
    serializer_class = serializers.DSRSerializer

    @action(
        methods=["POST"],
        detail=False,
        url_path="import",
        parser_classes=[GzipFileUploadParser],
    )
    def import_(self, request: "Request") -> Response:
        if dsr_file := request.data.get("file"):
            if instance := services.import_dsr(dsr_file):
                serializer = self.get_serializer(instance)
                return Response(serializer.data)
        raise ParseError()


class ResourcePercentileView(generics.ListAPIView):
    serializer_class = serializers.ResourcePercentileSerializer

    def get_queryset(self):
        kwargs = mappers.map_view_data_to_top_resources(
            query_params=self.request.query_params, kwargs=self.kwargs
        )
        return services.get_top_resources_by_percentile(**kwargs)
