from uuid import uuid4

import pytest
from unittest.mock import mock_open, patch


@pytest.fixture(scope="session", autouse=True)
def mock_open_file(request):
    file_contents = {
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace": "airflow-test-ns",
        "/var/run/secrets/kubernetes.io/serviceaccount/token": "eyJhbGciOiJSUzI1NiIsImtpZCI6IjYwZDMzNjQyOWMxYWQwNTNlMTFlNzBjM2JjNmFhY2E4Y2JkNTdiZWQifQ.eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjIl0sImV4cCI6MTczNzYyMzc0NSwiaWF0IjoxNzA2MDg3NzQ1LCJpc3MiOiJodHRwczovL29pZGMuZWtzLmV1LWNlbnRyYWwtMS5hbWF6b25hd3MuY29tL2lkL0U3MzVGNEUyM0Q0NjIzNzcyMTU2QTBGQURERDg3OEE3Iiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJhaXJmbG93LWV4IiwicG9kIjp7Im5hbWUiOiJhaXJmbG93LXdvcmtlci02YjRjOTk4Zjg1LWh2dm5mIiwidWlkIjoiNzdjYTY1NmUtZGU1NS00Mjk3LWIyYTgtNjRlOTMwZjYwZjliIn0sInNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJhaXJmbG93LXdvcmtlciIsInVpZCI6IjA2NTk0ZjY1LTFkYWYtNDExNS1iOGQyLWU5ZGEwZjkzOTUyZCJ9LCJ3YXJuYWZ0ZXIiOjE3MDYwOTEzNTJ9LCJuYmYiOjE3MDYwODc3NDUsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDphaXJmbG93LWV4OmFpcmZsb3ctd29ya2VyIn0.JBhXq0KZEHuMcq8ZjO9QJw3IDRThsGdPcaRpzIZ-tasczLsq4hthyqlhYxL1oieEO6zb6PA1lctv8LxlMxj-guwqSuPcW2VK00RRLM69MF5nq-urGsS5xqDG-oeQegiTS4_JSZcgwo2Fk3vXA1GNU30z-6nc9pfZ9gbLgXc3mJjBoEGkidQW30q9vZGgHS8H8ebAPjEcHkLuLaBNknsItxwcs2Eb59t4_L42a8DpgzakgZIZD8xKJotpOwDmtq-52Rs9iMBSL775BPzOIAKBYYIu0mBCEhFoPov0ivhaUQe7lS2f0ASqlODeqqpUcQOuWmoNNSi4dpeELK5dmubFFw"
    }

    def side_effect(file_path, *args, **kwargs):
        # Simulate different behaviors for different open targets
        content = file_contents.get(file_path, '')
        return mock_open(read_data=content).return_value

    # Use patch with side_effect to mock the built-in open() function
    with patch('builtins.open', side_effect=side_effect):
        yield


@pytest.fixture
def spark_driver_affinity_id():
    return str(uuid4())


@pytest.fixture(autouse=True)
def env_setup(monkeypatch, spark_driver_affinity_id):
    monkeypatch.setenv("SPARK_DRIVER_AFFINITY_ID", spark_driver_affinity_id)
