FROM python:3.8

WORKDIR /app/

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["portfolios/pessimistic_moonshot.py"]