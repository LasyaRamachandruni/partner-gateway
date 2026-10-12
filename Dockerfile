# One image for both services and the admin jobs:
#   pgw gateway | pgw vehicle-service | pgw db upgrade | pgw partners create ...
FROM python:3.12-slim AS build
WORKDIR /src
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY pyproject.toml README.md ./
COPY pgw ./pgw
COPY partner_sdk ./partner_sdk
RUN pip wheel --wheel-dir /wheels ".[gcp,otlp]"

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY --from=build /wheels /wheels
RUN pip install --no-index --find-links=/wheels partner-gateway[gcp,otlp] && rm -rf /wheels \
    && useradd --uid 10001 --no-create-home app
USER app
EXPOSE 8080 50051
ENTRYPOINT ["pgw"]
CMD ["gateway"]
