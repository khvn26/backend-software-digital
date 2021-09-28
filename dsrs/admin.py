from django import forms
from django.contrib import admin

from dsrs import models


@admin.register(models.Resource)
@admin.register(models.DSR)
class DeleteOnlyAdmin(admin.ModelAdmin):
    form = forms.ModelForm

    def has_add_permission(self, *_):
        return False
