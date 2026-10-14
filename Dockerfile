FROM python:3.10-slim
ENV TZ=Asia/Kolkata
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
# one socket per simulated device: run with a high open-files limit, e.g. docker run --ulimit nofile=65536:65536
CMD ["python", "main.py"]
