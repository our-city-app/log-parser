FROM python:3.7-alpine
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
ADD requirements.txt /code
RUN pip install -U -r requirements.txt
ADD log_parser /code/log_parser
CMD ["/code/log_parser/entrypoint.sh"]
