FROM python:3.10

RUN useradd -m -U app

WORKDIR /home/app
USER app
COPY benchmark_server /home/app/benchmark_server
COPY setup.* /home/app/

USER root
RUN pip install --no-cache-dir .

USER app
CMD ["python", "-m", "benchmark_server"]
