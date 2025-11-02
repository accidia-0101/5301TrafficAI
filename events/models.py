# models.py（最小事件表）
from django.db import models

class Event(models.Model):
    id = models.BigAutoField(primary_key=True)
    type = models.CharField(max_length=20, default="accident")   # 固定值
    camera_id = models.CharField(max_length=64, db_index=True)
    timestamp = models.DateTimeField(db_index=True)              # UTC
    confidence = models.FloatField(null=True, blank=True)        # 可空

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [models.Index(fields=["camera_id","timestamp"])]
