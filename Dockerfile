FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY pyproject.toml README.md ./
COPY pgw ./pgw
COPY partner_sdk ./partner_sdk
RUN pip install --no-cache-dir ".[gcp,otlp]"
RUN useradd --uid 10001 --no-create-home app
USER app
EXPOSE 8080 50051
ENTRYPOINT ["pgw"]
CMD ["gateway"]
