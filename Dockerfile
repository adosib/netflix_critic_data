# https://docs.astral.sh/uv/guides/integration/docker/#available-images
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set the working directory inside the container
WORKDIR /netflix_critic_data

COPY pyproject.toml /netflix_critic_data/pyproject.toml
COPY uv.lock /netflix_critic_data/uv.lock
COPY entrypoint.sh /netflix_critic_data/entrypoint.sh

# Install Node.js and npm (required for PythonMonkey)
RUN apt-get update && apt-get install -y npm

# Install the postgres client (not postgres db) to run psql commands
RUN apt-get install -y postgresql-client

# Synchronize dependencies defined in pyproject.toml
RUN uv sync

RUN chmod +x /netflix_critic_data/entrypoint.sh

CMD ["/netflix_critic_data/entrypoint.sh"]
