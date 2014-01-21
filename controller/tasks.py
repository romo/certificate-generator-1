from django.conf import settings
from django.db import transaction

import time
import logging
from statsd import statsd
import random
from django import db

from . import util
import gc
from statsd import statsd
import project_urls
from . single_instance_task import single_instance_task

from celery.task import periodic_task, task

import json
import urlparse
import xml.dom.minidom
import codecs

import cairosvg
from svglib.svglib import SvgRenderer
from reportlab.graphics import renderPDF

log = logging.getLogger(__name__)

@periodic_task(run_every=settings.TIME_BETWEEN_XQUEUE_PULLS)
@single_instance_task(60*10)
@transaction.commit_manually
def pull_from_xqueue():
  """
  Constant loop that pulls from queue and posts to grading controller
  """
  log.info(' [*] Pulling from xqueues...')

  #Define sessions for logging into xqueue and controller
  xqueue_session = util.xqueue_login()

  #Sleep for some time to allow other pull_from_xqueue processes to get behind/ahead
  time_sleep_value = random.uniform(0, .1)
  time.sleep(time_sleep_value)

  #Loop through each queue that is given in arguments
  for queue_name in settings.CERTIFICATE_QUEUES_TO_PULL_FROM:
      #Check for new submissions on xqueue, and send to controller
      pull_from_single_queue(queue_name,xqueue_session)


  # Log out of the controller session, which deletes the database row.
  #util.controller_logout(controller_session)

def pull_from_single_queue(queue_name,xqueue_session):
    try:
        #Get and parse queue objects
        success, queue_length= get_queue_length(queue_name,xqueue_session)
        log.info("success:{}  queue_length: {}".format(success,queue_length))
        #Check to see if the grading_controller server is up so that we can post to it


        #Only post while we were able to get a queue length from the xqueue, there are items in the queue, and the grading controller is up for us to post to.
        while success and queue_length>0:
            #Sleep for some time to allow other pull_from_xqueue processes to get behind/ahead
            time_sleep_value = random.uniform(0, .1)
            time.sleep(time_sleep_value)

            success, queue_item = get_from_queue(queue_name, xqueue_session)
            log.info("queue_item: {}".format(queue_item))
            success, content = util.parse_xobject(queue_item, queue_name)

            #Post to grading controller here!
            if  success:
                #TODO !!!
                #Post to controller
                # post_data = util._http_post(
                #     controller_session,
                #     urlparse.urljoin(settings.CERTIFICATE_CONTROLLER_INTERFACE['url'],
                #                      post_url),
                #     content,
                #     settings.REQUESTS_TIMEOUT,gm
                #     )

                with codecs.open('templates/certificate-template.svg', encoding='utf-8') as myfile:
                  svg=myfile.read().replace('\n', '')
                pdf = cairosvg.svg2pdf(svg)
                #doc = xml.dom.minidom.parseString(svg.encode( "utf-8" ))
                #svg = doc.documentElement
                #svgRenderer = SvgRenderer('templates/certificate-template.svg')
                #svgRenderer.render(svg)
                #drawing = svgRenderer.finish()
                #pdf = renderPDF.drawToString(drawing)
                s3_key = make_hashkey(xqueue_header)
                pdf_url = util.upload_to_s3(pdf,"test",s3_key)

                post_one_submission_back_to_queue(content,xqueue_session)

                statsd.increment("open_ended_assessment.grading_controller.pull_from_xqueue",
                                 tags=["success:True", "queue_name:{0}".format(queue_name)])
            else:
                log.error("Error getting queue item or no queue items to get.")
                statsd.increment("open_ended_assessment.grading_controller.pull_from_xqueue",
                                 tags=["success:False", "queue_name:{0}".format(queue_name)])

            success, queue_length= get_queue_length(queue_name, xqueue_session)
    except Exception:
        log.exception("Error getting submission")
        statsd.increment("open_ended_assessment.grading_controller.pull_from_xqueue",
                         tags=["success:Exception", "queue_name:{0}".format(queue_name)])


def post_one_submission_back_to_queue(content,xqueue_session):
    xqueue_header, xqueue_body = util.create_xqueue_header_and_body(submission)
    (success, msg) = util.post_results_to_xqueue(
        xqueue_session,
        json.dumps(content["xqueue_header"]),
        json.dumps(content["xqueue_body"]),
        )

    statsd.increment("open_ended_assessment.grading_controller.post_to_xqueue",
                     tags=["success:{0}".format(success)])

    if success:
        log.debug("Successful post back to xqueue! Success: {0} Message: {1} Xqueue Header: {2} Xqueue body: {3}".format(
            success,msg, xqueue_header, xqueue_body))
        submission.posted_results_back_to_queue = True
        submission.save()
    else:
        log.warning("Could not post back.  Error: {0}".format(msg))

def get_queue_length(queue_name,xqueue_session):
    """
    Returns the length of the queue
    """
    try:
        log.info("\n\txqueue_session: {}\n\turl: {}\n\tqueue_name: {}\n".format(xqueue_session,urlparse.urljoin(settings.XQUEUE_INTERFACE['url'], project_urls.XqueueURLs.get_queuelen),queue_name))
        success, response = util._http_get(xqueue_session,
                                           urlparse.urljoin(settings.XQUEUE_INTERFACE['url'], project_urls.XqueueURLs.get_queuelen),
                                           {'queue_name': queue_name})

        if not success:
            return False,"Invalid return code in reply"

    except Exception as e:
        log.critical("Unable to get queue length: {0}".format(e))
        return False, "Unable to get queue length."

    return True, response

def get_from_queue(queue_name,xqueue_session):
    """
    Get a single submission from xqueue
    """
    try:
        success, response = util._http_get(xqueue_session,
                                           urlparse.urljoin(settings.XQUEUE_INTERFACE['url'], project_urls.XqueueURLs.get_submission),
                                           {'queue_name': queue_name})
    except Exception as err:
        return False, "Error getting response: {0}".format(err)

    return success, response
