# Imagem única para pipeline e testes: Python 3.11 + Java 17 + PySpark/Delta.
# bookworm fixado porque traz o OpenJDK 17 nos repositórios oficiais.
FROM python:3.11-slim-bookworm

# mkdir: o pacote do JDK falha em imagens slim se o diretório de man pages não existir
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    # JAVA_HOME independente da arquitetura (amd64/arm64)
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    SPARK_JARS_IVY=/opt/ivy

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# Baixa os jars do Delta no build e valida que uma escrita Delta funciona.
# Assim a execução não depende de internet e falha cedo se algo estiver errado.
COPY docker/warmup_delta.py docker/
RUN python docker/warmup_delta.py

COPY . .

ENTRYPOINT ["python", "-m", "lending_pipeline"]
CMD ["--help"]
