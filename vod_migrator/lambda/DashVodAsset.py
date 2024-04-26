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

from mpegdash.parser import MPEGDASHParser
import os
from isodate import parse_duration
from datetime import datetime
from urllib.parse import urlparse
from Downloader import Downloader
from logging import Logger
import re

logger = None

# Supported Manifest
# Compact Time/Number with Timeline
# - AdaptationSet contains:
#   - Segment Template
#   - One or more representations (without Segment Template)
#
# Full Time/Number with Timeline
# - AdaptationSet contains:
#  - One or more representations
#  - Each representations contains Segment Template

class DashVodAsset:
  def __init__(self, loggerParam: Logger, masterManifest: str, downloader: Downloader, authHeaders=None):
    global logger
    logger = loggerParam
    self.masterManifest = masterManifest
    self.masterManifestContentType = None
    self.mediaSegmentList  = []
    self.commonPrefix = None
    self.allResource = None
    self.downloader = downloader
    self.authHeaders = authHeaders

    self.parseDashVodAsset()

  # Function will parse variant manifest and extract a list of all media and init segments
  # media and init segments will store absolute URLs for segments in mediaSegmentList
  def parseDashVodAsset( self ):

    mediaSegments = []

    # Retrieve Manifest
    (masterManifestBody, self.masterManifestContentType) = self.__getManifest( self.masterManifest )
    mpd = MPEGDASHParser.parse(masterManifestBody)
    mpdBaseUrl = os.path.dirname(self.masterManifest)

    # loop over periods
    periodCounter = 1
    for period in mpd.periods:

      logger.info("Starting processing Period %d ... " % periodCounter)
      # loop over all the adaptation sets in the period

      adaptationSetCounter = 1
      for adaptationSet in period.adaptation_sets:

        logger.info("Starting processing AdaptationSet %d with MimeType '%s'" % (adaptationSetCounter, adaptationSet.mime_type))

        listOfSegments = getAdaptationSetSegmentList(mpdBaseUrl, adaptationSet, period)
        mediaSegments.extend(listOfSegments)

        logger.info("Finished processing AdaptationSet %d." % adaptationSetCounter)

        # Increment AdaptationSet Counter
        adaptationSetCounter = adaptationSetCounter + 1

      logger.info("Finished processing Period %d." % periodCounter)

      # Increment Period Counter
      periodCounter = periodCounter + 1

    # Identify common base URL for all resources
    allSegments = list(mediaSegments)
    allSegments.append(self.masterManifest)

    # Duplicates need to be removed from the list of all segments. Duplicate can occur when processing multiperiod DASH
    # streams where the init file does not change across period boundaries. To do this the list of segments will be
    # converted to a set and then back to a list.
    allSegmentsSet = set(allSegments)
    uniqueSegments = list(allSegmentsSet)

    # Set common prefix
    # The common prefix must end with '/' to indicate this is a path and does not include
    # the start of the name of the files. For example, if all the resources of an asset start
    # with 'asset1' and the content is stored in 'my/asset/path' the common prefix should be
    # 'my/asset/path.' not 'my/asset/path/asset1'
    commonPrefix = os.path.commonprefix( uniqueSegments )
    if not commonPrefix.endswith('/'):
      # Strip off anything to the right of the last '/' because this component represents
      # the common string at the start of the filenames
      unwantedPathSuffix = os.path.basename(commonPrefix)
      pathSuffix = re.compile( unwantedPathSuffix + '$' )
      commonPrefix = pathSuffix.sub('', commonPrefix)
    self.commonPrefix = commonPrefix

    self.mediaSegmentList = mediaSegments
    self.allResources = uniqueSegments

    return

  # Function provides a hook to inspect and potentially modify the retrieved data for each
  # resource prior to the resource being written to storage.
  # In most cases this will not be required but there are certain circumstances where
  # assets harvests from a live stream may need to be slightly modified to work optimally
  # in a VOD context.
  def manipulateResourceBeforeWritingToStorage( self, resource, data, contentType ):
    return (data, contentType)

  def __getManifest( self, url: str):

    contentType = None
    try:
      (statusCode, urlPayload, contentType, errorMessage) = self.downloader.loadUrl('dashVodAsset', url, self.authHeaders)
    except IOError as urlErr:
      logger.error("Exception occurred while attempting to retrieve manifest: %s" % url )
      logger.info(repr(urlErr))
      urlPayload = None
      raise Exception({
        "httpError": statusCode,
        "responseBody": repr(urlErr),
        "error": "Exception occurred while attempting to retrieve manifest",
        "url": url}
      )

    if statusCode is None or statusCode != 200:

      # Format status code
      if statusCode is None:
        statusCode = "None"
      else:
        statusCode = str(statusCode)

      logger.error('http error:%s.  fetching: %s' % (statusCode, url) )
      raise Exception({
        "httpError": statusCode,
        "responseBody": urlPayload.decode('utf-8') if urlPayload else None,
        "error": errorMessage if errorMessage else "Error received when retrieving manifest",
        "url": url}
      )

    if not( urlPayload is None ):
      urlPayload = urlPayload.decode('utf-8')

    return ( urlPayload, contentType )


def getAdaptationSetSegmentList(mpdBaseUrl, adaptationSet, period):

  mediaSegments = []
  
  for representation in adaptationSet.representations:
    logger.info("Processing Representation %s:" % representation.id)

    # Get segment Template
    # Segment template may be defined in representation or at the Adaptation set level
    segmentTemplates = None
    if representation.segment_templates:
      segmentTemplates = representation.segment_templates
    elif adaptationSet.segment_templates:
      segmentTemplates = adaptationSet.segment_templates
    else:
      errorMessage = "Unable to find Segment Template for Representation %s" % representation.id
      logger.error(errorMessage)
      raise Exception({ "error": errorMessage })

    # Assumption there is only one segment template per adaptation set
    if len(segmentTemplates) > 1:
      errorMessage = "Unsupported DASH Manifest format. Maximum of one segment template per adaptations set"
      logger.error(errorMessage)
      raise Exception({ "error": errorMessage })

    ############################
    # Process Media Files
    ############################

    # Extract Media Segment template and fill in any required parameters (e.g. representation id)
    segmentTemplate = segmentTemplates[0]
    mediaSegmentTemplate = segmentTemplate.media

    if "$RepresentationID$" in mediaSegmentTemplate:
      mediaSegmentTemplate = mediaSegmentTemplate.replace("$RepresentationID$", str(representation.id))

    if "$Bandwidth$" in mediaSegmentTemplate:
      mediaSegmentTemplate = mediaSegmentTemplate.replace("$Bandwidth$", str(representation.bandwidth))

    logger.info("Media Segment Template: %s" % mediaSegmentTemplate)

    # Generate a list of media files to be downloaded
    # Segment Templates do not exist for some renditions (e.g. 'image/jpeg')
    # For these renditions a segment timeline is inferred from the SegmentTemplate
    mediaSegmentTimes = None
    if segmentTemplate.segment_timelines:
      mediaSegmentTimes = getSegmentTimeline( segmentTemplate )
    else:
      mediaSegmentTimes = getInferredSegmentTimeline( segmentTemplate.start_number, segmentTemplate.timescale, segmentTemplate.duration, period.duration )

    mediaSegmentsForRepresentation = getMediaSegmentList( mediaSegmentTemplate, segmentTemplate.start_number, mediaSegmentTimes, mpdBaseUrl )

    ############################
    # Process Init File (it exists for this rendition)
    ############################
    if segmentTemplate.initialization:

      # Extract init segment template and fill in any required parameters (e.g. representation id)
      initSegmentTemplate = segmentTemplate.initialization
      if "$RepresentationID$" in initSegmentTemplate:
        initSegmentTemplate = initSegmentTemplate.replace("$RepresentationID$", str(representation.id))
      if "$Bandwidth$" in initSegmentTemplate:
        initSegmentTemplate = mediaSegmentTemplate.replace("$Bandwidth$", str(representation.bandwidth))

      # Some non-AWS packagers include the $Time$ value in init segments but set the value to 'i'
      # This is a workaround to be compatible with these packagers
      if "$Time$" in initSegmentTemplate:
        initSegmentTemplate = mediaSegmentTemplate.replace("$Time$", "i")

      logger.info("Init Segment Template: %s" % initSegmentTemplate)
      # Add init file to resource list
      absInitSegmentTemplate = normalizeUrl(mpdBaseUrl + '/' + initSegmentTemplate)

      # Append init files to list of media files to be downloaded
      mediaSegmentsForRepresentation.append(absInitSegmentTemplate)
    else:
      logger.info("Skipping init file as there is no init for '%s' representation" % representation.id)

    # Append list of files to be downloaded as part of this adaptation set
    mediaSegments.extend(mediaSegmentsForRepresentation)

  return mediaSegments

# Generate list of media segments
def getMediaSegmentList( mediaSegmentTemplate, startNumber, mediaSegmentTimes, mpdBaseUrl ):
  mediaSegments = []

  for t in mediaSegmentTimes:
    resource = mediaSegmentTemplate

    # Handle both Time with Timeline and Number with timeline mpd formats
    if "$Time$" in resource:
      # Time with Timeline mpd
      resource = resource.replace("$Time$", str(t))
    else:
      # Number with Timeline mpd
      resource = resource.replace("$Number$", str(startNumber))
      startNumber = startNumber + 1

    absResource = normalizeUrl(mpdBaseUrl + '/' + resource)
    mediaSegments.append(absResource)

  return mediaSegments

# Uses the segment template to generate a list of segment times
def getSegmentTimeline( segmentTemplate: str ):

  segmentTimelines = segmentTemplate.segment_timelines

  # Iterate over the segment components to create a list of the times for segments to download 
  mediaSegmentTimes = []
  segmentTimelineComponents = segmentTimelines[0].Ss

  t = None
  for segmentTimelineComponent in segmentTimelineComponents:

    # Set time if set on segment
    if segmentTimelineComponent.t != None:
      t = segmentTimelineComponent.t # time
    d = segmentTimelineComponent.d # duration
    r = segmentTimelineComponent.r # repeats

    # add first segment
    # print("MediaSegmentTime: %d" % t)
    mediaSegmentTimes.append(t)

    # add any repeat segments
    if not (r is None):
      for x in range(1,r+1):
        # print("MediaSegmentTime (r): %s" % str(t + x*d))
        mediaSegmentTimes.append(t + x*d)
    else:
      r = 0 # explicitly set the repeats to zero rather than None to make the time calculation easier

    # The value for time needs to be incremented by the sum of all the durations for the repeated segments
    # if there were no repeats of the segment the t value is just incremented by the durations
    t = t + (r+1)*d

  return mediaSegmentTimes

# Infers a segment timeline if not explicitly defined
def getInferredSegmentTimeline( startNumber, timescale, segmentTemplateDuration, periodDuration ):

  # Approach used here is to calculate segment size (i.e. segment template duration divided by timescale)
  # Dividing the duration of the period by the segment size should give the correct number of assets

  # Calculate segement size
  segmentSize = float(segmentTemplateDuration)/float(timescale)

  # Get period duration
  periodDuration = parse_duration(periodDuration).total_seconds()

  # Calculcate number of segments in period
  numberSegments = int(periodDuration / segmentSize)

  # Create array listing the segment numbers
  segmentTimelineNumbers = list(range(startNumber, startNumber+numberSegments))

  return segmentTimelineNumbers

# Normalizes url and removes additional '..' notations
def normalizeUrl( url: str ):

  parsedUrl = urlparse(url)
  absPath = os.path.normpath( parsedUrl.path )
  absUrl = "%s://%s%s" % (parsedUrl.scheme, parsedUrl.netloc, absPath)

  # Include query parameter if present
  if parsedUrl.query:
    absUrl += "?%s" % parsedUrl.query

  return absUrl
