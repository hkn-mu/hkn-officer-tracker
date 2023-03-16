import logging
import os
from datetime import datetime

import pandas as pd
import requests


RESPONSES_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTqjtYipKDHEQwx5OIho6pC_WOwKyBHMtYIqm4my9PvGrTZlHMnoYq-F68RxFhb2Hjt39HdIHB6QfpV/pub?gid=719878135&single=true&output=csv"
EVENTS_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTqjtYipKDHEQwx5OIho6pC_WOwKyBHMtYIqm4my9PvGrTZlHMnoYq-F68RxFhb2Hjt39HdIHB6QfpV/pub?gid=930946685&single=true&output=csv"
OUT_PATH = os.path.dirname(os.path.realpath(__file__))


def count_attendance(responses: pd.DataFrame, events: pd.DataFrame, column: str) -> pd.DataFrame:
    filtered_events = events[events["Activity Type"] == column]
    filtered_responses = responses[["HKN Handle", "Week", "Secret Word"]][responses["Activity Type"] == column]

    df_dict = dict(tuple(filtered_responses.groupby(["HKN Handle"])))

    return pd.DataFrame(
        [(k, v.merge(filtered_events, how="inner", on=["Week", "Secret Word"]).shape[0]) for k, v in df_dict.items()],
        columns=["HKN Handle", f"{column}s Attended"],
    )


def main() -> None:
    logging.info("Fetching latest HKN attendance data")
    responses_req = requests.get(RESPONSES_URL, timeout=10)
    events_req = requests.get(EVENTS_URL, timeout=10)

    with open(os.path.join(os.getcwd(), "responses.csv"), 'wb') as f:
        f.write(responses_req.content)

    with open(os.path.join(os.getcwd(), "events.csv"), 'wb') as f:
        f.write(events_req.content)

    logging.info("Reading data into DataFrames")
    responses = pd.read_csv(os.path.join(os.getcwd(), "responses.csv"))
    events = pd.read_csv(os.path.join(os.getcwd(), "events.csv"))

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

    attendance = hm_attendance \
        .merge(cr_attendance, how="outer", on="HKN Handle") \
        .merge(ts_attendance, how="outer", on="HKN Handle") \
        .merge(cm_attendance, how="outer", on="HKN Handle") \
        .merge(gm_attendance, how="outer", on="HKN Handle") \
        .merge(icd_attendance, how="outer", on="HKN Handle") \
        .merge(qsm_attendance, how="outer", on="HKN Handle") \
        .fillna(0)
    attendance.iloc[:, 1:] = attendance.iloc[:, 1:].astype(int)

    logging.info("Saving attendance file as \"attendance.csv\"")
    attendance.sort_values("HKN Handle").to_csv(os.path.join(OUT_PATH, "attendance.csv"), index=False)


if __name__ == "__main__":
    ts = datetime.now().isoformat(timespec="seconds")
    logging.basicConfig(
        filename=os.path.join(OUT_PATH, f"attendance-tracker-{ts}.log"),
        encoding="utf-8",
        level=logging.INFO
    )
    main()
