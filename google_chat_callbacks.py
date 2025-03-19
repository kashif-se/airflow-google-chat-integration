# Author: Kashif Sohail
# Repository: https://github.com/kashif-se/airflow-google-chat-integration
# Description: This code sends alerts to Google Chat based on task success or failure in an Airflow environment.
# Date: June 1, 2023
# License: MIT License
# Dependencies: requests, airflow
# Usage: Call the task_fail_alert(context) function for task failure alerts and task_success_alert(context) function for task success alerts.

import requests
import traceback
from airflow.hooks.base_hook import BaseHook

# Change the name of webhook connection as you set in airflow.
GCHAT_CONNECTION = "gchat_webhook"


def task_fail_alert(context):
    """
    Sends an alert to Google Chat in case of task failure.
    Args:
        context (dict): Context object containing information about the task instance.
    """

    # forming the run_id, we will use it later as a unique thread_id so that we can push exception details as thread
    # while exception message will be posted as a card in Space.
    run_id = str(context.get("task_instance").dag_id)+"-"+str(context.get("task_instance").run_id).replace(
        "+", "-").replace(":", "-")

    print("task_fail_alert()")
    exception = context.get("exception")
    formatted_exception = str(exception)
    try:
        tb = None if type(exception) == str else exception.__traceback__
        formatted_exception = "".join(
            traceback.format_exception(etype=type(
                exception), value=exception, tb=tb)
        )
    except:
        pass

    # form a card to represent alert in a better way.
    body = {
        'cardsV2': [{
            'cardId': 'createCardMessage',
            'card': {
                'header': {
                    'title': "{task} is failed after {tries} tries".format(task=context.get("task_instance").task_id, tries=context.get("task_instance").prev_attempted_tries),
                    'subtitle': context.get("task_instance").dag_id,
                    'imageUrl': "https://img.icons8.com/fluency/48/delete-sign.png"
                },
                'sections': [
                    {
                        'widgets': [
                            {
                                "textParagraph": {
                                    "text": f"<b>Execution Time:</b> <time>{context.get('logical_date')}</time>",
                                }
                            },
                            {
                                "textParagraph": {
                                    "text": f"<b>Task Duration: </b> {context.get('task_instance').duration}s",
                                }
                            },
                            {
                                "textParagraph": {
                                    "text": f"<b>Exception:</b> <i>{str(exception)[:150]}</i>",
                                }
                            },
                            {
                                'buttonList': {
                                    'buttons': [
                                        {
                                            'text': 'View Logs',
                                            'onClick': {
                                                'openLink': {
                                                    'url': context.get("task_instance").log_url
                                                }
                                            }
                                        },
                                        {
                                            'text': 'Mark Success',
                                            'onClick': {
                                                'openLink': {
                                                    'url': context.get("task_instance").mark_success_url
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        }]
    }

    thread_ref = f"&threadKey={run_id}&messageReplyOption=REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
    full_url = _get_webhook_url(GCHAT_CONNECTION, thread_ref)
    print("sending alert card")
    _make_http_request(body, full_url)

    print("sending exception as a thread")
    body = {
        "text": f"""<users/all>
                    ```{formatted_exception}```"""
    }
    _make_http_request(body, full_url)


def task_success_alert(context):
    """
    Sends an alert to Google Chat in case of task success.
    Args:
        context (dict): Context object containing information about the task instance.
    """

    print("task_success_alert()")

    body = {
        'cardsV2': [{
            'cardId': 'createCardMessage',
            'card': {
                'header': {
                    'title': "{task} execution successfull".format(task=context.get("task_instance").task_id, tries=context.get("task_instance").prev_attempted_tries),
                    'subtitle': context.get("task_instance").dag_id,
                    'imageUrl': "https://img.icons8.com/fluency/48/checkmark.png"
                },
                'sections': [
                    {
                        'widgets': [
                            {
                                "textParagraph": {
                                    "text": f"<b>Execution Time:</b> <time>{context.get('logical_date')}</time>",
                                }
                            },
                            {
                                "textParagraph": {
                                    "text": f"<b>Task Duration: </b> {context.get('task_instance').duration}s",
                                }
                            },

                            {
                                'buttonList': {
                                    'buttons': [
                                        {
                                            'text': 'View Logs',
                                            'onClick': {
                                                'openLink': {
                                                    'url': context.get("task_instance").log_url
                                                }
                                            }
                                        },

                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        }]
    }
    full_url = _get_webhook_url(GCHAT_CONNECTION)
    if not full_url.startswith('http'):
        full_url = f"https://{full_url}"
        
    print("sending alert card")
    _make_http_request(body, full_url)


def _make_http_request(body, full_url):
    """
    Sends an HTTP POST request with the provided body to the given URL.
    Args:
        body (dict): The request body.
        full_url (str): The URL to send the request to.
    """
    r = requests.post(
        url=full_url,
        json=body,
        headers={"Content-type": "application/json"},
    )
    print(r.status_code, r.ok)


def _get_webhook_url(connection_id: str, thread_ref: str = ""):
    """
    Retrieves the webhook URL for the specified connection ID.
    Args:
        connection_id (str): The connection ID.
        thread_ref (str): The optional thread reference.
    Returns:
        str: The constructed URL.
    """
    gchat_connection = BaseHook.get_connection(connection_id)
    full_url = f"{gchat_connection.host}{gchat_connection.password}{thread_ref}"
    return full_url
