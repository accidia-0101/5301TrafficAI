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

from django.urls import path

from api.views import play_view, stop_view, alerts_stream, ask_view

urlpatterns = [
    path("api/play", play_view),
    path("api/stop", stop_view),
    path("sse/alerts", alerts_stream),
    path("api/ask", ask_view),

]
