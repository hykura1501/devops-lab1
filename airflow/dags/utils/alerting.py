import requests
import json
from datetime import datetime


class AlertManager:
    def __init__(self, webhook_url=None):
        # Get webhook URL from environment variable or parameter
        import os

        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL", "")

    def send_slack_alert(self, title, message, severity="info"):
        color_map = {"info": "#36a64f", "warning": "#ff9900", "error": "#ff0000", "critical": "#8b0000"}
        payload = {
            "attachments": [
                {
                    "color": color_map.get(severity, "#36a64f"),
                    "title": title,
                    "text": message,
                    "footer": "DataOps Monitoring",
                    "ts": int(datetime.now().timestamp()),
                }
            ]
        }

        # For testing without Slack webhook
        print("=== ALERT TRIGGERED ===")
        print(f"Severity: {severity}")
        print(f"Title: {title}")
        print(f"Message: {message}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        print("======================")

        # Check if webhook URL is configured
        if not self.webhook_url or self.webhook_url.strip() == "":
            print("WARNING: SLACK_WEBHOOK_URL is not configured. Alert will not be sent to Slack.")
            print("To enable Slack notifications, set SLACK_WEBHOOK_URL environment variable.")
            return False

        try:
            response = requests.post(
                self.webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=10
            )
            print(f"Slack API Response: Status={response.status_code}, Body={response.text}")
            if response.status_code == 200:
                print("Alert successfully sent to Slack!")
                return True
            else:
                print(f"Slack returned error: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Failed to send alert: {e}")
            return False

    def alert_pipeline_failure(self, dag_id, task_id, error_message):
        title = f"Pipeline Failure: {dag_id}"
        message = f"Task {task_id} failed\n\nError: {error_message}"
        return self.send_slack_alert(title, message, "error")

    def alert_test_failure(self, test_name, failure_count):
        title = "Data Quality Alert"
        message = f"Test {test_name} failed\n\nFailures: {failure_count}"
        return self.send_slack_alert(title, message, "warning")

    def alert_slow_pipeline(self, dag_id, execution_time, threshold):
        title = f"Slow Pipeline: {dag_id}"
        message = f"Execution time: {execution_time:.2f}s (threshold: {threshold}s)"
        return self.send_slack_alert(title, message, "warning")
