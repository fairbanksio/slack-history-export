import os
import json
import sys
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

client = WebClient(token=os.environ["SLACK_API_TOKEN"])

page_size = 100  # number of messages per page
max_pages = 100  # maximum number of pages to retrieve

# Call the conversations.list method using the WebClient
# This returns a list of all channels in the workspace
try:
    result = client.conversations_list()
    channels = result["channels"]
    channels = sorted(channels, key=lambda k: k["name"])
except SlackApiError as e:
    print("Error retrieving channel list: {}".format(e))
    sys.exit(1)

# iterate over each channel and scrape its history
for channel in channels:
    channel_id = channel["id"]
    channel_name = channel["name"]

    messages = []

    # load previous messages from JSON file
    filename = f"{channel_name}.json"
    previous_messages = []
    try:
        with open(filename, "r") as f:
            previous_messages = json.load(f)

    except FileNotFoundError:
        pass

    try:
        # Call the conversations.history method using the WebClient
        # We'll retrieve up to 100 pages of messages, or until we reach the end of the channel history
        for i in range(max_pages):
            try:
                result = client.conversations_history(
                    channel=channel_id,
                    limit=page_size,
                    cursor=result.get("response_metadata", {}).get("next_cursor"),
                )
            except SlackApiError as e:
                error_message = e.response["error"]
                print(
                    f"Error getting conversation history for #{channel_name}: {error_message}"
                )
                break

            # check if response_metadata exists before accessing it
            if "response_metadata" not in result or not result["has_more"]:
                # we've reached the end of the channel history
                break

            messages += result["messages"]

    except SlackApiError as e:
        error_message = e.response["error"]
        print(
            "Error getting conversation history for #{}: {}".format(
                channel_name, error_message
            )
        )
        continue

    # filter out messages that already exist in previous_messages list
    new_messages = []
    previous_message_ids = {
        msg["ts"] for msg in previous_messages
    }  # create a set of message IDs
    for msg in messages:
        if (
            msg["ts"] not in previous_message_ids
        ):  # check if the ID is already in the set
            # print(f"Found new message: {msg['text']}\n")
            new_messages.append(msg)
            previous_message_ids.add(msg["ts"])  # add the new message's ID to the set

    # sort messages by timestamp
    previous_messages.sort(key=lambda msg: float(msg["ts"]))
    new_messages.sort(key=lambda msg: float(msg["ts"]))

    # write new_messages to a JSON file
    if new_messages:
        with open(filename, "w") as f:
            json.dump(previous_messages + new_messages, f, indent=2)
        print(f"Saved {len(new_messages):,d} new message(s) to {filename}\n")
    else:
        print(f"No new messages found for #{channel_name}\n")

    # wait for 2 seconds before moving on to the next channel
    time.sleep(2)
