FROM python:3.8

WORKDIR /app/

COPY ../requirements.txt .

RUN pip install -U -r requirements.txt

COPY .. .

ENV PYTHONUNBUFFERED=1

CMD ["python3", "-m", "trading.portfolios.mean_reversion"]
