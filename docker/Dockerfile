FROM python:3.9-slim as base

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

FROM base AS build

RUN pip install pipenv
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential python-dev \
    libpcre3 libpcre3-dev

COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy

FROM base AS runtime

COPY --from=build /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

COPY . .

ENV DJANGO_SETTINGS_MODULE "digital.settings.production"

EXPOSE 80

RUN mkdir -p /var/www/static && python manage.py collectstatic 

ENTRYPOINT ["uwsgi", "uwsgi.ini"]
