FROM python:3.9.2

ADD main.py .
add requirements.txt .

RUN pip install -r requirements.txt

CMD  ["python", "./main.py"]

