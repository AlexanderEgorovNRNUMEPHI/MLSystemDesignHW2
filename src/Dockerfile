# pull official base image
FROM python:3.9-slim

WORKDIR /opt/src/app
# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# copy requirements file
COPY ./requirements.txt /requirements.txt

# install dependencies
COPY requirements.txt /requirements.txt
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --no-cache-dir --upgrade pip setuptools wheel &&\
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --no-cache-dir -r  /requirements.txt 
# copy project

COPY . /opt/src/app/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]