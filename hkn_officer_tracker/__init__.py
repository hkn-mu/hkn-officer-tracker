from flask import Flask, request
from dotenv import load_dotenv
from slackbot import parse_response, get_requirements, send_message


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
