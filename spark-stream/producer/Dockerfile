FROM python:3.6-alpine

WORKDIR /usr/src/app

RUN apk --update add python py-pip openssl ca-certificates py-openssl wget

COPY requirements.txt ./
RUN apk --update add --virtual build-dependencies libffi-dev openssl-dev python-dev py-pip build-base \
  && pip install --upgrade pip \
  && pip install -r requirements.txt \
  && apk del build-dependencies


COPY . .

CMD ["python", "-u", "./producer.py"]
