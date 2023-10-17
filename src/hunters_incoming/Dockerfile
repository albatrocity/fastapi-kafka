FROM python:3.11 as requirements-stage

ENV PYTHONDONTWRITEBYTECODE 1  
ENV PYTHONUNBUFFERED 1  
ENV PYTHONPATH=/code/app  

 
WORKDIR /tmp

RUN pip install poetry==1.6.1

# 
COPY ./hunters_incoming/pyproject.toml ./poetry.lock* /tmp/

# 
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

# 
FROM python:3.11

# 
WORKDIR /code

# 
COPY --from=requirements-stage /tmp/requirements.txt /code/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 
COPY ./hunters_incoming /code/app
COPY ./lib /code/app/lib

# 
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]