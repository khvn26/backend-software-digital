from typing import get_args

from django.db import models

from dsrs.types import DSRStatus


class Territory(models.Model):
    name = models.CharField(max_length=48)
    code_2 = models.CharField(max_length=2, unique=True)
    code_3 = models.CharField(max_length=3)
    local_currency = models.ForeignKey(
        "Currency", related_name="territories", on_delete=models.CASCADE
    )

    class Meta:
        db_table = "territory"
        verbose_name = "territory"
        verbose_name_plural = "territories"
        ordering = ("name",)
        indexes = (models.Index(fields=["code_2"]),)


class Currency(models.Model):
    name = models.CharField(max_length=48)
    symbol = models.CharField(max_length=4)
    code = models.CharField(max_length=3, unique=True)

    class Meta:
        db_table = "currency"
        verbose_name = "currency"
        verbose_name_plural = "currencies"
        indexes = (models.Index(fields=["code"]),)


class DSR(models.Model):
    class Meta:
        db_table = "dsr"

    STATUS_ALL = tuple((arg, arg.upper()) for arg in get_args(DSRStatus))

    path = models.CharField(max_length=256)
    period_start = models.DateField(null=False)
    period_end = models.DateField(null=False)

    status = models.CharField(
        choices=STATUS_ALL, default=STATUS_ALL[0][0], max_length=48
    )

    territory = models.ForeignKey(
        Territory, related_name="dsrs", on_delete=models.CASCADE
    )
    currency = models.ForeignKey(
        Currency, related_name="dsrs", on_delete=models.CASCADE
    )

    def __str__(self):
        return self.path


class Resource(models.Model):
    dsr = models.ForeignKey(DSR, related_name="resources", on_delete=models.CASCADE)
    dsp_id = models.CharField(max_length=30)
    title = models.CharField(max_length=255)
    artists = models.CharField(max_length=255)
    isrc = models.CharField(max_length=12)
    usages = models.IntegerField()
    revenue = models.DecimalField(decimal_places=20, max_digits=40)

    def __str__(self):
        return f"[{self.isrc}] {self.artists} — {self.title}"
