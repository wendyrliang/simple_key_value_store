FROM python:3
ADD myapp.py /
RUN pip install flask
RUN pip install requests
RUN pip install flask_executor
ENV FLASK_APP="myapp.py"
RUN FLASK_APP=myapp.py
EXPOSE 8080
CMD ["flask", "run","--host=0.0.0.0", "--port=8080"]
