from rest_framework import fields, serializers

from dsrs import models


class TerritorySerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Territory
        fields = (
            "name",
            "code_2",
        )


class CurrencySerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Currency
        fields = (
            "name",
            "code",
        )


class DSRSerializer(serializers.ModelSerializer):
    territory = TerritorySerializer()
    currency = CurrencySerializer()

    class Meta:
        model = models.DSR
        fields = (
            "id",
            "path",
            "period_start",
            "period_end",
            "status",
            "territory",
            "currency",
        )


class ResourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Resource
        fields = (
            "dsr",
            "dsp_id",
            "title",
            "artists",
            "isrc",
            "usages",
            "revenue",
        )


class ResourcePercentileSerializer(serializers.ModelSerializer):
    dsr_ids = fields.ListField(fields.IntegerField())

    class Meta:
        model = models.Resource
        fields = (
            "dsp_id",
            "title",
            "artists",
            "isrc",
            "usages",
            "revenue",
            "dsr_ids",
        )


class ResourcePercentileQuerySerializer(serializers.Serializer):
    territory = fields.CharField(min_length=2, max_length=2, required=False)
    period_start = fields.DateField(required=False)
    period_end = fields.DateField(required=False)
