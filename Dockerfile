FROM python:3.8-slim

WORKDIR /srv/www

COPY requirements.txt requirements.txt

RUN python -m pip install -r requirements.txt

COPY . .

EXPOSE 8080

CMD [ "python3", "-m" , "gunicorn", "--workers", "1", "--threads", "8", "--bind", "0.0.0.0:8080", "main:app"]