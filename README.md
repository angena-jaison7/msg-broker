# Telemetry Message Broker

## Architecture Highlights
* **Infrastructure as Code:** RabbitMQ is containerized via Docker for isolated, reproducible deployments.
* **Request-Scoped Dependency Injection:** The FastAPI backend utilizes connection lifecycle management (`Depends`) to open and safely close broker connections per HTTP request, preventing memory leaks and connection pool exhaustion.
* **Interactive API Documentation:** Integrated Swagger UI for testing producer and consumer endpoints.

## Prerequisites
Ensure you have the following installed on your local machine:
* [Docker Desktop](https://www.docker.com/products/docker-desktop) (Running in the background)
* [Python 3.9+](https://www.python.org/downloads/)
* [Git](https://git-scm.com/)

---

## 1. Local Environment Setup

First, clone the repository and navigate into the project directory:
```bash
git clone <YOUR_REPOSITORY_URL_HERE>
cd msg-broker

Set up the Python Virtual Environment:
It is highly recommended to isolate the project dependencies. Do not commit the venv folder to version control.

Bash
python -m venv venv
Activate the Virtual Environment:

Windows (PowerShell): .\venv\Scripts\Activate.ps1

Mac/Linux: source venv/bin/activate

Install Dependencies:

Bash
pip install -r requirements.txt
2. Infrastructure Setup (RabbitMQ)
The message broker is run inside a Docker container. Ensure Docker Desktop is open, then spin up the container using the provided docker-compose.yml file:

Bash
docker-compose up -d
(Note: The -d flag runs the container in detached mode in the background).

Verify the container is running:

Bash
docker ps
RabbitMQ Management Credentials:
You can monitor live queues, exchanges, and connections via the RabbitMQ Management Web UI.

URL: http://localhost:15672

Username: admin

Password: historian

3. Running the FastAPI Application
With the broker running, start the Uvicorn ASGI server to launch the FastAPI backend:

Bash
uvicorn src.api:app --reload
(Note: The --reload flag enables auto-reloading for local development).

4. API Usage & Testing (Swagger UI)
FastAPI automatically generates interactive API documentation. You can test the end-to-end Producer and Consumer flow directly from your browser.

Access the Dashboard:
Navigate to http://127.0.0.1:8000/docs
http://localhost:8000/docs

Testing the Flow:

Produce a Message (Simulate an Instrument):
Locate the POST /alerts endpoint. Click Try it out, enter a test JSON payload (e.g., {"reading": -60.5}), and click Execute. This publishes a message to the freezer_alerts queue.

Consume a Message (Simulate Downstream Software):
Locate the GET /alerts endpoint. Click Try it out and Execute to safely pull the alert from the RabbitMQ queue.