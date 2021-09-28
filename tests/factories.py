from datetime import date

import factory
from factory import fuzzy


class CurrencyFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = "dsrs.Currency"


class TerritoryFactory(factory.django.DjangoModelFactory):
    local_currency = factory.SubFactory(CurrencyFactory)

    class Meta:
        model = "dsrs.Territory"


class DSRFactory(factory.django.DjangoModelFactory):
    period_start = fuzzy.FuzzyDate(start_date=date(2020, 1, 1))
    period_end = fuzzy.FuzzyDate(start_date=date(2020, 2, 1))

    territory = factory.SubFactory(TerritoryFactory)
    currency = factory.SubFactory(CurrencyFactory)

    class Meta:
        model = "dsrs.DSR"
