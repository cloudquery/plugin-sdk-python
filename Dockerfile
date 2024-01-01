FROM python:3.12-slim

WORKDIR /app

# Copy the code and install dependencies
COPY requirements.txt .
COPY setup.cfg .
COPY setup.py .
COPY cloudquery cloudquery
COPY main.py .
RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 7777

ENTRYPOINT ["python3", "main.py"]

CMD ["serve", "--address", "[::]:7777", "--log-format", "json", "--log-level", "info"]