#!/bin/bash
# Slack Notification Helper Script
# Usage: ./slack_notification.sh <title> <message> <color> <webhook_url>

set -e

TITLE="${1:-Deployment Notification}"
MESSAGE="${2:-No message provided}"
COLOR="${3:-good}"  # good, warning, danger
WEBHOOK_URL="${4:-$SLACK_WEBHOOK_URL}"

if [ -z "$WEBHOOK_URL" ]; then
    echo "Warning: SLACK_WEBHOOK_URL not set. Skipping Slack notification."
    exit 0
fi

# Build Slack payload
PAYLOAD=$(cat <<EOF
{
    "text": "$TITLE",
    "attachments": [{
        "color": "$COLOR",
        "text": "$MESSAGE",
        "footer": "DataOps Pipeline",
        "ts": $(date +%s)
    }]
}
EOF
)

# Send to Slack
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    -H 'Content-type: application/json' \
    --data "$PAYLOAD" \
    "$WEBHOOK_URL")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" -eq 200 ]; then
    echo "Slack notification sent successfully"
    exit 0
else
    echo "Failed to send Slack notification. HTTP code: $HTTP_CODE"
    echo "Response: $BODY"
    exit 1
fi

