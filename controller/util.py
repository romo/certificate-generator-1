from django.conf import settings
from functools import wraps
import json
import logging
import requests
import urlparse
import project_urls
import re

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from django.http import HttpResponse
from django.contrib.auth.models import User, Group, Permission

from django.db import connection

import traceback
import hashlib
from lxml.html.clean import Cleaner

log = logging.getLogger(__name__)

_INTERFACE_VERSION = 1


def parse_xreply(xreply):
    """
    Parse the reply from xqueue. Messages are JSON-serialized dict:
        { 'return_code': 0 (success), 1 (fail)
          'content': Message from xqueue (string)
        }
    """

    try:
        xreply = json.loads(xreply)
    except ValueError:
        error_message =  "Could not parse xreply."
        log.error(error_message)
        return (False, error_message)

    #This is to correctly parse xserver replies and internal success/failure messages
    if 'return_code' in xreply:
        return_code = (xreply['return_code']==0)
        content = xreply['content']
    elif 'success' in xreply:
        return_code = xreply['success']
        content=xreply
    else:
        return False, "Cannot find a valid success or return code."

    if return_code not in [True,False]:
        return (False, 'Invalid return code.')


    return return_code, content


def parse_xobject(xobject, queue_name):
    """
    Parse a queue object from xqueue:
        { 'return_code': 0 (success), 1 (fail)
          'content': Message from xqueue (string)
        }
    """
    try:
        xobject = json.loads(xobject)

        header = json.loads(xobject['xqueue_header'])
        header.update({'queue_name': queue_name})
        body = json.loads(xobject['xqueue_body'])

        content = {'xqueue_header': json.dumps(header),
                   'xqueue_body': json.dumps(body)
        }
    except ValueError:
        error_message = "Unexpected reply from server."
        log.error(error_message)
        return (False, error_message)

    return True, content

def login(session, url, username, password):
    """
    Login to given url with given username and password.
    Use given request session (requests.session)
    """

    log.debug("Trying to login to {0} with user: {1} and pass {2}".format(url,username,password))
    response = session.post(url,
        {'username': username,
         'password': password,
        }
    )

    if response.status_code == 500 and url.endswith("/"):
        response = session.post(url[:-1],
                                {'username': username,
                                 'password': password,
                                 }
        )


    response.raise_for_status()
    log.debug("login response from %r: %r", url, response.json)
    (success, msg) = parse_xreply(response.content)
    return success, msg


def _http_get(session, url, data=None):
    """
    Send an HTTP get request:
    session: requests.session object.
    url : url to send request to
    data: optional dictionary to send
    """
    if data is None:
        data = {}
    try:
        r = session.get(url, params=data)
    except requests.exceptions.ConnectionError:
        error_message = "Cannot connect to server."
        log.error(error_message)
        return (False, error_message)

    if r.status_code == 500 and url.endswith("/"):
        r = session.get(url[:-1], params=data)

    if r.status_code not in [200]:
        return (False, 'Unexpected HTTP status code [%d]' % r.status_code)
    if hasattr(r, "text"):
        text = r.text
    elif hasattr(r, "content"):
        text = r.content
    else:
        error_message = "Could not get response from http object."
        log.exception(error_message)
        return False, error_message
    return parse_xreply(text)


def _http_post(session, url, data, timeout):
    '''
    Contact grading controller, but fail gently.
    Takes following arguments:
    session - requests.session object
    url - url to post to
    data - dictionary with data to post
    timeout - timeout in settings

    Returns (success, msg), where:
        success: Flag indicating successful exchange (Boolean)
        msg: Accompanying message; Controller reply when successful (string)
    '''

    try:
        r = session.post(url, data=data, timeout=timeout, verify=False)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        error_message = 'Could not connect to server at %s in timeout=%f' % (url, timeout)
        log.error(error_message)
        return (False, error_message)

    if r.status_code == 500 and url.endswith("/"):
        r = session.post(url[:-1], data=data, timeout=timeout, verify=False)

    if r.status_code not in [200]:
        error_message = "Server %s returned status_code=%d' % (url, r.status_code)"
        log.error(error_message)
        return (False, error_message)

    if hasattr(r, "text"):
        text = r.text
    elif hasattr(r, "content"):
        text = r.content
    else:
        error_message = "Could not get response from http object."
        log.exception(error_message)
        return False, error_message

    return (True, text)


def post_results_to_xqueue(session, header, body):
    """
    Post the results from a grader back to xqueue.
    Input:
        session - a requests session that is logged in to xqueue
        header - xqueue header.  Dict containing keys submission_key and submission_id
        body - xqueue body.  Arbitrary dict.
    """
    request = {
        'xqueue_header': header,
        'xqueue_body': body,
    }

    (success, msg) = _http_post(session, settings.XQUEUE_INTERFACE['url'] + project_urls.XqueueURLs.put_result, request,
        settings.REQUESTS_TIMEOUT)

    return success, msg

def xqueue_login():
    session = requests.session()
    xqueue_login_url = urlparse.urljoin(settings.XQUEUE_INTERFACE['url'], project_urls.XqueueURLs.log_in)
    (success, xqueue_msg) = login(
        session,
        xqueue_login_url,
        settings.XQUEUE_INTERFACE['django_auth']['username'],
        settings.XQUEUE_INTERFACE['django_auth']['password'],
    )

    return session

def create_xqueue_header_and_body(submission):
    xqueue_header = {
        'submission_id': submission.xqueue_submission_id,
        'submission_key': submission.xqueue_submission_key,
    }

    score_and_feedback = submission.get_all_successful_scores_and_feedback()
    score = score_and_feedback['score']
    feedback = score_and_feedback['feedback']
    grader_type=score_and_feedback['grader_type']
    success=score_and_feedback['success']
    grader_id = score_and_feedback['grader_id']
    submission_id = score_and_feedback['submission_id']
    rubric_scores_complete = score_and_feedback['rubric_scores_complete']
    rubric_xml = score_and_feedback['rubric_xml']
    xqueue_body = {
        'feedback': feedback,
        'score': score,
        'grader_type' : grader_type,
        'success' : success,
        'grader_id' : grader_id,
        'submission_id' : submission_id,
        'rubric_scores_complete' : rubric_scores_complete,
        'rubric_xml' : rubric_xml,
    }

    return xqueue_header, xqueue_body


def _error_response(msg, version, data=None):
    """
    Return a failing response with the specified message.

    Args:
        msg: used as the 'error' key
        version: specifies the protocol version
        data: if specified, a dict that's included in the response
    """
    response = {'version': version,
                'success': False,
                'error': msg}

    if data is not None:
        response.update(data)
    return HttpResponse(json.dumps(response), mimetype="application/json")


def _success_response(data, version):
    """
    Return a successful response with the specified data.
    """
    response = {'version': version,
                'success': True}
    response.update(data)
    return HttpResponse(json.dumps(response), mimetype="application/json")


def sanitize_html(text):
    try:
        cleaner = Cleaner(style=True, links=True, add_nofollow=False, page_structure=True, safe_attrs_only=False, allow_tags = ["img", "a"])
        clean_html = cleaner.clean_html(text)
        clean_html = re.sub(r'</p>$', '', re.sub(r'^<p>', '', clean_html))
    except Exception:
        clean_html = text
    return clean_html

def upload_to_s3(string_to_upload, path, name):
    '''
    Upload file to S3 using provided keyname.

    Returns:
        public_url: URL to access uploaded file
    '''
    try:
        conn = S3Connection(settings.AWS_ACCESS_KEY_ID, settings.AWS_SECRET_ACCESS_KEY)
        bucketname = settings.S3_BUCKETNAME
        try:
            bucket = conn.create_bucket(bucketname.lower())
        except Exception:
            bucket = conn.get_bucket(bucketname.lower())
        prefix = getattr(settings, 'S3_PATH_PREFIX')
        path = '{0}/{1}'.format(prefix, path)

        k = Key(bucket)
        k.key = '{path}/{name}'.format(path=path, name=name)
        k.set_contents_from_string(string_to_upload)
        public_url = k.generate_url(60*60*24*365) # URL timeout in seconds.

        return True, public_url
    except Exception:
        error = "Could not connect to S3."
        log.exception(error)
        return False, error

def make_hashkey(seed):
    '''
    Generate a hashkey (string)
    '''
    h = hashlib.md5()
    h.update(str(seed))
    return h.hexdigest()
