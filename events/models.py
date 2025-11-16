# -----------------------------------------------------------------------------
# Copyright (c) 2025
#
# Authors:
#   Liruo Wang
#       School of Electrical Engineering and Computer Science,
#       University of Ottawa
#       lwang032@uottawa.ca
#
# All rights reserved.
# -----------------------------------------------------------------------------
import uuid

from django.db import models
from pgvector.django import VectorField


class Camera(models.Model):
    """Map to the database table 'cameras' (multi-camera support)."""

    camera_id = models.CharField(max_length=64, primary_key=True)

    class Meta:
        db_table = "cameras"  # ✅ 对应你手动建的表名

    def __str__(self):
        return self.camera_id




class Event(models.Model):
    """Map to the database table 'events' (event records)."""
    event_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,     # ✅ 自动生成唯一UUID
        editable=False
    )
    timestamp = models.DateTimeField()
    camera = models.ForeignKey("Camera", on_delete=models.CASCADE, db_column="camera_id")
    type = models.CharField(max_length=16)
    weather = models.CharField(max_length=8, null=True, blank=True)
    confidence = models.FloatField()
    evidence_text = models.TextField()
    embedding = VectorField(dimensions=768, null=True, blank=True)

    class Meta:
        db_table = "events"


    def __str__(self):
        return f"{self.camera_id} | {self.type} | {self.confidence:.2f}"
