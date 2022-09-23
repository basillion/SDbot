FROM python:3.10-slim-bullseye
COPY *.* /app/
RUN pip install -r /app/requirements.txt
CMD ["python3", "/app/SDjson.py"]
