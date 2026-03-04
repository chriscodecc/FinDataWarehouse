FROM python:3.12-slim AS builder
#sudo chmod 666 /var/run/docker.sock
WORKDIR /app
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/* 
    # Datein und Verzeichnisse löschen -rf recusiv (auch Ordner) f force

COPY requirements.txt .
# installiert req.txt ins standard verzeichniss (/home/vscode/.local) des Users und löscht die installationsdatein 
RUN pip install --user --no-cache-dir -r requirements.txt

FROM python:3.12-slim
WORKDIR /app
# --- NEU: User 'vscode' anlegen ---
RUN groupadd --gid 1000 vscode \
    && useradd --uid 1000 --gid 1000 -m vscode \
    && apt-get update \
    && apt-get install -y sudo \
    && echo vscode ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode
    
# ----------------------------------
# libpq5 runtime für postgres
RUN apt-get update && apt-get install -y libpq5 && rm -rf /var/lib/apt/lists/*
# local inhalte aus build kopieren
COPY --from=builder /root/.local /home/vscode/.local
RUN chown -R vscode:vscode /home/vscode/.local 
RUN groupadd -g 999 docker_host \
    && usermod -aG docker_host vscode
COPY src/ ./src/
ENV PATH=/home/vscode/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

USER vscode

CMD ["python", "src/main.py"]