# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import boto3
import urllib3
import time
import re
from urllib.parse import urlparse
from urllib.parse import parse_qs
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.endpoint import URLLib3Session
from pprint import pprint

session = boto3.Session()
credentials = session.get_credentials()

SIGV4_SIGN_REQUESTS = None
SIGV4_REQUEST_REGION = None

class Downloader:
  def __init__(self, loggerParam, numThreads):
    global logger
    logger = loggerParam

    # Initialize urllib3 Pool Manager
    self.poolManager = urllib3.PoolManager( maxsize=numThreads )
    self.urllib3session = URLLib3Session( max_pool_connections=numThreads )


  def loadUrl(self, caller, url, authHeaders):

    retryCount = 3
    retryInterval = 2
    attempt = 0
    lastStatusCode = None

    while attempt < retryCount:
      (statusCode, urlPayload, contentType, errorMessage) = self.__loadUrlWorker(caller, url, authHeaders)
      if urlPayload is not None:
        return (statusCode, urlPayload, contentType, errorMessage)
      attempt += 1
      time.sleep(retryInterval)
      lastStatusCode = statusCode

    logger.error("'%s': failed to load after %d attempts: %s", caller, retryCount, url)
    return (lastStatusCode, None, None, errorMessage)

  def __loadUrlWorker(self, caller, url, authHeaders):

    urlPayload = None
    contentType = None
    errorMessage = None

    if SIGV4_SIGN_REQUESTS is None:
      self.inferSigV4Signing( url )

    try:
      if SIGV4_SIGN_REQUESTS:
        logger.info("Retrieving object using SigV4 signed request: %s" % url)
        response = self.__createAndSendSigv4GetRequest(credentials, url)
      else:
        logger.info("Retrieving object using unsigned request: %s" % url)
        response = self.poolManager.request( "GET", url, headers=authHeaders )
    except IOError as urlErr:
      urlPayload = None
      print('I/O error fetching', url)
      print(urlErr)
      return (None, None, None, None)

    # Here if urlopen succeeded.  Check http result code.  Anything other than
    # 200 (success) is returned to caller as an error.
    statusCode = response.status_code if SIGV4_SIGN_REQUESTS else response.status


    if statusCode != 200:

      logger.info("Failed to download '%s'" % url)

      # If this was a sigv4 request check if there was a useful error message
      # If there was a useful error message put it in the errorMessage to be returned
      if response.headers['Content-Type'] == 'application/json':
        data = response.content if SIGV4_SIGN_REQUESTS else response.data
        errorMessage = data.decode('utf8').replace("'", '"')
        logger.error('Error message: %s', errorMessage)

      logger.error('http error: %d - fetching: %s', statusCode, url)
    else:

      # Get the payload.
      urlPayload = response.content if SIGV4_SIGN_REQUESTS else response.data
      contentType = response.headers['Content-Type']
      logger.info("Resource successfully retrieved: %s" % url)


    return (statusCode, urlPayload, contentType, errorMessage)

  def __createAndSendSigv4GetRequest(self, credentials, url):

    # Parse the URL to get the host and path
    parsedUrl = urlparse(url)
    host = parsedUrl.hostname
    bareUrl = "%s://%s%s" % ( parsedUrl.scheme, host, parsedUrl.path )

    # Parse query parameters
    queryParams = parse_qs(parsedUrl.query)
    for queryKey in queryParams.keys():
      queryParams[queryKey] = queryParams[queryKey][0]

    # Verify hostname ends with 'amazonaws.com' and is a MediaPackage V2 URL
    # raise console warning if conditions not met
    if not re.match(r'.+\.mediapackagev2\..+\.amazonaws\.com$', host):
      logger.warning("Warning: Hostname '%s' does not end with 'amazonaws.com'" % host)
      logger.warning("When using SigV4 headers it is anticate requests are being made directly")
      logger.warning("to an AWS Endpoint (often mediapackagev2 or S3)")
      logger.warning("If you are not using AWS Elemental MediaPackage V2 or Amazon S3 the use of SigV4 auth is not required.")

    # Create SigV4 request object
    request = AWSRequest(
      method='GET',
      url = bareUrl,
      params=queryParams,
      headers={
          'Host': host,
      },
    )

    # Create signer and sign request
    signer = SigV4Auth(credentials, 'mediapackagev2', SIGV4_REQUEST_REGION)
    signer.add_auth(request)

    # Create the SigV4 request object
    response = self.urllib3session.send(request.prepare())

    return response

  def inferSigV4Signing( self, url ):

    global SIGV4_SIGN_REQUESTS
    global SIGV4_REQUEST_REGION

    if url.split('.')[3] == 'mediapackagev2':
      SIGV4_SIGN_REQUESTS = True
      logger.debug("SigV4 request signing enabled as URL is a MediaPackage V2 Endpoint")
      logger.debug("SIGV4_SIGN_REQUESTS: %s" % SIGV4_SIGN_REQUESTS)
    else:
      logger.debug("SigV4 request signing disabled as URL is not a MediaPackage V2 Endpoint")

    if SIGV4_SIGN_REQUESTS:
      SIGV4_REQUEST_REGION = url.split('.')[4]
      logger.info("SIGV4_REQUEST_REGION: %s" % SIGV4_REQUEST_REGION)