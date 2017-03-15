from __future__ import unicode_literals
from django.db import models


class KvsEntry(models.Model):
    key = models.CharField(max_length=250)
    value = models.TextField()
    clock = models.TextField()
    time = models.FloatField()

    @classmethod
    # for entering a key, value pair
    def create_entry(cls, key, value):
        entry = cls(key, value)
        return entry
