FROM python:3.8-slim

WORKDIR /srv/www

COPY requirements.txt requirements.txt

RUN python -m pip install -r requirements.txt

COPY . .

ENV FLASK_APP=main.py

EXPOSE 5000

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]