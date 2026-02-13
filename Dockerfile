FROM python:3.12-slim AS builder

WORKDIR /app
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

FROM python:3.12-slim
WORKDIR /app
# --- NEU: User 'vscode' anlegen ---
RUN groupadd --gid 1000 vscode \
    && groupadd -g 999 docker_host \ 
    && useradd --uid 1000 --gid 1000 -m -G sudo,docker_host vscode \
    && apt-get update \
    && apt-get install -y sudo libpq5 \
    && echo vscode ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode \
    && rm -rf /var/lib/apt/lists/*
# ----------------------------------
RUN apt-get update && apt-get install -y libpq5 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /root/.local /home/vscode/.local
RUN chown -R vscode:vscode /home/vscode/.local 
# COPY --from=builder /root/.local /root/.local
COPY src/ ./src/
ENV PATH=/home/vscode/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

USER vscode

CMD ["python", "src/main.py"]