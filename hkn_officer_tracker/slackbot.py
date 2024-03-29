"""Slackbot for HKN officer requirement tracking."""

# pylint: disable=logging-fstring-interpolation
# pylint: enable=logging-format-interpolation

import datetime
import logging
import os
import time
import urllib.parse
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from flask import Flask, request, current_app
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

OUT_PATH = Path(os.path.dirname(os.path.realpath(__file__))) / "data"
RESPONSES = OUT_PATH / "responses.csv"
EVENTS = OUT_PATH / "events.csv"
ATTENDANCE = OUT_PATH / "attendance.csv"


def parse_response(stream: bytes) -> tuple[str, str, str]:
    """
    Extracts the desired user from the POST request.
    """
    stream_string = stream.decode("utf-8")
    response_dict = urllib.parse.parse_qs(stream_string)
    return (
        response_dict["channel_id"][0],
        response_dict["user_id"][0],
        response_dict["user_name"][0],
    )


def get_requirements(user_id: str, user_name: str) -> dict:
    """
    Returns a formatted message of the user's requirements.
    """
    attendance_df = fetch_attendance()
    payload = attendance_df[attendance_df["HKN Handle"] == user_name].to_dict(
        orient="records"
    )[0]
    attendance_block = "\n".join([f"*{k}*: {v}" for k, v in payload.items()][1:])
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Hello <@{user_id}>, here is your current progress on HKN officer requirements:",
            },
        },
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": attendance_block}},
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "If you have any questions/bug reports about this feature, feel free to ping <@bryanngo>.",
            },
        },
    ]
    return blocks


def send_message(channel_id: str, user_id: str, requirements: str) -> None:
    """
    Sends the requirements message back through the Slack API.
    """
    slack_bot_token = os.getenv("SLACK_BOT_TOKEN")
    client = WebClient(token=slack_bot_token)

    try:
        _ = client.chat_postEphemeral(
            channel=channel_id,
            blocks=requirements,
            text="placeholder",
            user=user_id,
        )
    except SlackApiError as error:
        print(error.response["error"])


def count_attendance(
    responses: pd.DataFrame, events: pd.DataFrame, column: str
) -> pd.DataFrame:
    """
    Helper function to count HKN attendance.
    """
    filtered_events = events[events["Activity Type"] == column]
    filtered_responses = responses[["HKN Handle", "Week", "Secret Word"]][
        responses["Activity Type"] == column
    ]

    valid_attendance = pd.merge(
        filtered_responses,
        filtered_events,
        left_on=["Week", "Secret Word"],
        right_on=["Week", "Secret Word"],
        how="inner",
    )

    counted_attendance = valid_attendance.groupby("HKN Handle", as_index=False).count()
    return counted_attendance[["HKN Handle", "Week"]].rename(
        columns={"Week": f"{column}s Attended"}
    )


def cache_attendance() -> None:
    """
    Reads the responses and events forms from Google Sheets, processes the
    attendance file, then caches it to a file on disk.
    """
    responses_url = os.getenv("RESPONSES_URL")
    events_url = os.getenv("EVENTS_URL")

    logging.info("Fetching latest HKN attendance data")
    responses_req = requests.get(responses_url, timeout=10)
    events_req = requests.get(events_url, timeout=10)

    with open(RESPONSES, "wb") as fp:
        fp.write(responses_req.content)

    with open(EVENTS, "wb") as fp:
        fp.write(events_req.content)

    logging.info("Reading data into DataFrames")
    responses = pd.read_csv(RESPONSES)
    events = pd.read_csv(EVENTS)

    responses["HKN Handle"] = responses["HKN Handle"].str.strip().str.lower()
    responses["Secret Word"] = responses["Secret Word"].str.strip().str.lower()
    events["Secret Word"] = events["Secret Word"].str.strip().str.lower()

    logging.info("Calculating attendance")
    hm_attendance = count_attendance(responses, events, "HM")
    cr_attendance = count_attendance(responses, events, "Cookie Run")
    ts_attendance = count_attendance(responses, events, "Teaching Session")
    cm_attendance = count_attendance(responses, events, "CM")
    gm_attendance = count_attendance(responses, events, "GM")
    icd_attendance = count_attendance(responses, events, "Inter-Committee Duty")
    qsm_attendance = count_attendance(responses, events, "QSM")

    attendance = (
        hm_attendance.merge(cr_attendance, how="outer", on="HKN Handle")
        .merge(ts_attendance, how="outer", on="HKN Handle")
        .merge(cm_attendance, how="outer", on="HKN Handle")
        .merge(gm_attendance, how="outer", on="HKN Handle")
        .merge(icd_attendance, how="outer", on="HKN Handle")
        .merge(qsm_attendance, how="outer", on="HKN Handle")
        .fillna(0)
    )
    attendance[attendance.columns[1:]] = attendance[attendance.columns[1:]].astype(int)

    logging.info('Saving attendance file as "attendance.csv"')
    attendance.sort_values("HKN Handle").to_csv(ATTENDANCE, index=False)


def fetch_attendance() -> pd.DataFrame:
    """
    Fetches the attendance from the cache on disk.
    """
    sheet = OUT_PATH / "attendance.csv"
    one_week = datetime.timedelta(weeks=1)
    if os.path.getmtime(RESPONSES) - time.time() > one_week.total_seconds():
        cache_attendance()
    return pd.read_csv(sheet)


def create_app():
    app = Flask(__name__)

    with app.app_context():
        load_dotenv()

    @app.route("/", methods=["POST"])
    def do_POST() -> None:
        """
        Handles any POST requests by constructing a message and
        sending it to the Slack API to send to the user.
        """
        if request.method == "POST":
            content_len = int(request.headers.get("Content-Length"))
            channel_id, user_id, user_name = parse_response(
                request.stream.read(content_len)
            )
            requirements = get_requirements(user_id, user_name)
            send_message(channel_id, user_id, requirements)
        return "", 200

    return app


def main() -> None:
    """
    Main driver for the Slackbot.
    """
    # logging.basicConfig(level=logging.DEBUG)
    app = create_app()
    app.run(debug=True)


if __name__ == "__main__":
    main()
