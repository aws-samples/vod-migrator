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

import os
from pprint import pprint
from urllib.parse import urlparse
from Downloader import Downloader
from logging import Logger
import re

logger = None

class HlsVodAsset:
  def __init__(self, loggerParam: Logger, masterManifest: str, downloader: Downloader, authHeaders=None):
    global logger
    logger = loggerParam
    self.masterManifest = masterManifest
    self.masterManifestContentType = None
    self.variantManifests   = None
    self.variantManifestsData = {}
    self.mediaSegmentList  = []
    self.commonPrefix = None
    self.allResources = None
    self.allResourcesExceptMasterManifest = None
    self.downloader = downloader
    self.authHeaders = authHeaders

    self.parseHlsVodAsset()

  # Function will parse variant manifest and extract a list of all media and init segments
  # media and init segments will store absolute URLs for segments in mediaSegmentList
  def parseHlsVodAsset( self ):

    # Retrieve Master Manifest
    (masterManifestBody, self.masterManifestContentType) = self.__getManifest( self.masterManifest )

    # Parse Master Manifest
    self.variantManifests = self.__parseMasterManifest( self.masterManifest, masterManifestBody )

    # For each variant manifest
    for variant in self.variantManifests:

      # Retrieve Variant Manifest
      (variantManifestBody, variantContentType) = self.__getManifest( variant )
      self.variantManifestsData[variant] = {
        "body": variantManifestBody,
        "contentType": variantContentType
      }

      # Parse Variant Manifest
      segments = self.__parseVariantManifest( variant, variantManifestBody )
      self.mediaSegmentList.extend(segments)

    # Determine commonPrefix across all resource
    allResources = [ self.masterManifest ]
    allResources.extend(self.variantManifests)
    allResources.extend(self.mediaSegmentList)

    # Duplicates need to be removed from the list of all segments.
    # This may not strictly be necessary for HLS/CMAF streams but cannot hurt.
    allResourcesSet = set(allResources)
    uniqueResources = list(allResourcesSet)

    self.allResources = uniqueResources

    # Create list of all resources except master manifest
    self.allResourcesExceptMasterManifest = [ resource for resource in self.allResources if resource != self.masterManifest ]    

    # Set common prefix
    # The common prefix must end with '/' to indicate this is a path and does not include
    # the start of the name of the files. For example, if all the resources of an asset start
    # with 'asset1' and the content is stored in 'my/asset/path' the common prefix should be
    # 'my/asset/path/' not 'my/asset/path/asset1'
    commonPrefix = os.path.commonprefix( uniqueResources )
    if not commonPrefix.endswith('/'):
      # Strip off anything to the right of the last '/' because this component represents
      # the common string at the start of the filenames
      unwantedPathSuffix = os.path.basename(commonPrefix)
      pathSuffix = re.compile( unwantedPathSuffix + '$' )
      commonPrefix = pathSuffix.sub('', commonPrefix)
    self.commonPrefix = commonPrefix

    return

  # Function provides a hook to inspect and potentially modify the retrieved data for each
  # resource prior to the resource being written to storage.
  # In most cases this will not be required but there are certain circumstances where
  # assets harvests from a live stream may need to be slightly modified to work optimally
  # in a VOD context.
  def manipulateResourceBeforeWritingToStorage( self, resource, data, contentType ):

    # extract query parameters from resource url
    queryStrings = urlparse(resource).query

    # Check if the resource is the master manifest
    if resource == self.masterManifest and queryStrings:
      logger.info("Modifying multi-variant manifest to remove query parameters in references to variant manifests")
      multiVariantManifestString = data.decode('utf-8')
      multiVariantManifestString = multiVariantManifestString.replace( "?"+queryStrings ,"" )
      data = str.encode(multiVariantManifestString)

    return (data, contentType)


  def __getManifest( self, url: str ):

    contentType = None
    try:
      (statusCode, urlPayload, contentType, errorMessage) = self.downloader.loadUrl('hlsVodAsset', url, self.authHeaders)
    except IOError as urlErr:
      logger.error("Exception occurred while attempting to retrieve manifest: %s" % url )
      logger.error(repr(urlErr))
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

    else:
      # Some packagers set the manifest type incorrectly.
      # This needs to be corrected if the content type is 'binary/octet-stream'
      if contentType == 'binary/octet-stream':
        logger.info("Content type was '%s', overriding to '%s'" % (contentType, 'application/x-mpegURL'))
        contentType = 'application/x-mpegURL'

    if not( urlPayload is None ):
      urlPayload = urlPayload.decode('utf-8')
      # Check if it's an HLS manifest
      if urlPayload[0:7] != '#EXTM3U':
        raise Exception({
          "httpError": statusCode,
          "responseBody": urlPayload,
          "error": "Not a HLS manifest",
          "url": url}
        )

    return ( urlPayload, contentType )


  # Function will parse master manifest and extract a list of all the variant manifests
  # Variant manifest URLs will be stored in list in 'variantManifest'
  def __parseMasterManifest( self, masterManifestUrl: str, masterManifestBody: bytes ):

    variantsDict = {}
    for line in masterManifestBody.splitlines():
      logger.debug("[parseMasterManifest] Line: %s" % line)
      name = None

      # Parse line starting with EXT-X-MEDIA
      # e.g. EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio_0",CHANNELS="2",NAME="und",LANGUAGE="und",DEFAULT=YES, \
      #      AUTOSELECT=YES,URI="bf4fc289ea7a4a9a8030bfdfb6dd8180/75449fe7ed1a492880193067011
      if line.startswith('#EXT-X-MEDIA:') or line.startswith('#EXT-X-I-FRAME-STREAM-INF:'):
        line = line.split(':', 1)[1]
        mediaDict = {}

        # Add EXT-X-MEDIA properties to data set 
        for keyVal in re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', line):
          (key, val) = keyVal.split('=', 1)
          mediaDict[key] = val.strip('"')
        if mediaDict['URI']:
          name = mediaDict['URI']
        else:
          # Skip lines not containing variant manifest
          next

      elif line == "":
        # Skip blank lines
        continue

      # Parse lines which do not start with a comment
      # e.g. ../../../bf4fc289ea7a4a9a8030bfdfb6dd8180/75449fe7ed1a49288019306701174382/index_1_0.ts
      elif line[0] != '#':
        name = line

      else:
        # Skip lines not including a variant manifest
        continue

      # Add key to dict if it has not been seen before
      logger.debug("[parseMasterManifest] Name: %s" % name)
      absoluteUrl = name
      if name and name.startswith("http"):
        absoluteUrl = name
      elif name:
        absoluteUrl = normalizeUrl("%s/%s" % (os.path.dirname(masterManifestUrl), name))
      logger.debug("[parseMasterManifest] AbsoluteUrl: %s" % absoluteUrl)

      if not (absoluteUrl is None or absoluteUrl in variantsDict.keys()):
        variantsDict[absoluteUrl] = 1

    variants = list(variantsDict.keys())

    return variants


  # Function will parse variant manifest and extract a list of all media and init segments
  # media and init segments will store absolute URLs for segments in mediaSegmentList
  def __parseVariantManifest( self, variantManifestUrl: str, variantManifestBody: bytes ):

    segmentsDict = {}
    for line in variantManifestBody.splitlines():
      name = None

      # Parse line starting with EXT-X-MEDIA
      # e.g. #EXT-X-MAP:URI="../../../a595fd669f4349e1846efee6e27ccfa8/bba5843ebf8f41619348551669b17f47/index_video_1_init.mp4"
      if line.startswith('#EXT-X-MAP:') or line.startswith('#EXT-X-I-FRAME-STREAM-INF:'):
        line = line.split(':', 1)[1]
        mediaDict = {}

        # Add EXT-X-MEDIA properties to data set 
        for keyVal in re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', line):
          (key, val) = keyVal.split('=', 1)
          mediaDict[key] = val.strip('"')
        name = mediaDict['URI']

      # Parse lines which do not start with a comment
      # e.g. ../../../bf4fc289ea7a4a9a8030bfdfb6dd8180/75449fe7ed1a49288019306701174382/index_1_0.ts
      elif line[0] != '#':
        name = line

      # Add key to dict if it has not been seen before
      absoluteUrl = name
      if name and name.startswith("http"):
        absoluteUrl = name
      elif name:
        absoluteUrl = normalizeUrl("%s/%s" % (os.path.dirname(variantManifestUrl), name))

      if not (absoluteUrl is None or absoluteUrl in segmentsDict.keys()):
        segmentsDict[absoluteUrl] = 1
      
    segments = list(segmentsDict.keys())

    return segments

# Normalizes url and removes additional '..' notations
def normalizeUrl( url: str ):

  parsedUrl = urlparse(url)
  absPath = os.path.normpath( parsedUrl.path )
  absUrl = "%s://%s%s" % (parsedUrl.scheme, parsedUrl.netloc, absPath)

  # Include query parameter if present
  if parsedUrl.query:
    absUrl += "?%s" % parsedUrl.query

  return absUrl
