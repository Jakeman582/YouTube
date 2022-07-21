import os
import math
import json
import re
import sys
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def check_present_files(data_directory):

	path = Path(data_directory)
	return [file.stem for file in path.iterdir() if file.is_file()]

def build_service():

	key = os.environ["YOUTUBE_API_KEY"]
	return build("youtube", "v3", developerKey = key)

def get_channel(user_name, channel_part, service):

	if(user_name.startswith("UC")):
		channel_request = service.channels().list(
			part = channel_part, id = user_name)
	else:
		channel_request = service.channels().list(
			part = channel_part, forUsername = user_name)

	try:
		channel_list = channel_request.execute()
		return channel_list['items'][0]
	except HttpError as e:
		raise

def get_video_ids(playlist_part, playlist_id, playlist_token, results_per_request, service):

	request = service.playlistItems().list(
		part = playlist_part, 
		playlistId = playlist_id, 
		pageToken = playlist_token, 
		maxResults = results_per_request)

	try:
		return request.execute()
	except HttpError as e:
		raise

def get_videos(video_part, video_id, service):

	request = service.videos().list(part = video_part, id = video_id)

	try:
		response = request.execute()
		return response['items']
	except HttpError as e:
		raise


def get_video_comment_thread_page(video_id = None, service = None, 
								  page_token = ""):
	"""Retrieve a page of upto the currently supported maximum number of 
	comment threads from the specified YouTube video ID.

	:param video_id: The id of the video from which to collect comments, 
		defaults to None
	:type video_id: string
	:param service: The YouTube API service object used to make API calls, 
		defaults to None
	:type service: object
	:param page_token: The token specifying which page of comment threads to 
		return (the first page is represented by an empty string), defaults to 
		""
	:type page_token: string, optional
	:raises HttpError: Any HTTP error resulting from communication with the 
		YouTube API
	:raises AttributeError: In case the Google YouTube API documentation is 
		wrong (as it has been discovered during development)
	:raises TypeError: If none of the required arguments (video_id, service) 
		are not supplied
	:return: An API response with a list of commentThread objects
	:rtype: object
	"""

	if(not video_id):
		raise TypeError(
			"get_video_comment_thread_page(): argument 'video_id' not set"
		)

	if(not service):
		raise TypeError(
			"get_video_comment_thread_page(): argument 'service' not set"
		)
	
	# Default settings for the request
	comment_thread_part = "id,snippet"
	text_format = "plainText"
	maximum_comment_threads_per_page = 100

	# Construct the request
	comment_thread_request = service.commentThreads().list(
		part = comment_thread_part,
		videoId = video_id,
		maxResults = maximum_comment_threads_per_page,
		textFormat = text_format,
		pageToken = page_token
	)

	# Attempt to get the results
	try:
		return comment_thread_request.execute()
	except HttpError as e:
		raise e
	except AttributeError as e:
		raise e

def get_comments(comment_thread_part, video_id, service):

	# The API allows a maximum of 100 comments per 'page' of results
	comment_threads_per_page = 100

	# We don't want to deal with the HTML formatting
	text_format = "plainText"

	# Because of quota limits, we may have to return only a partial
	# amount of comments
	comments = []

	try:

		# Get the first top-level comments and store their ids
		response = get_video_comment_thread_page(video_id, service)
		top_level_ids = set()

		for comment_thread in response['items']:
			comment = comment_thread['snippet']['topLevelComment']
			top_level_ids.add(comment['id'])
			comments.append(get_comment_attributes(comment, video_id))

		# There may be multiple pages of comment threads (top-level comments)
		while('nextPageToken' in response):

			page_token = response['nextPageToken']
			response = get_video_comment_thread_page(video_id, service, page_token)

			for comment_thread in response['items']:
				comment = comment_thread['snippet']['topLevelComment']
				top_level_ids.add(comment['id'])
				comments.append(get_comment_attributes(comment, video_id))

		# Get every comment submitted as a reply to a top-level comment
		for parent_id in top_level_ids:

			comment_request = service.comments().list(
				part = comment_thread_part,
				parentId = parent_id,
				maxResults = comment_threads_per_page,
				textFormat = text_format
			)

			comment_response = comment_request.execute()

			for comment in comment_response['items']:
				comments.append(get_comment_attributes(comment, video_id))

			# Make sure to get any additional replies
			while('nextPageToken' in comment_response):

				page_token = comment_response['nextPageToken']
				comment_request = service.comments().list(
					part = comment_thread_part,
					parentId = parent_id,
					maxResults = comment_threads_per_page,
					textFormat = text_format,
					pageToken = page_token
				)

				comment_response = comment_request.execute()

				for comment in comment_response['items']:
					comments.append(get_comment_attributes(comment, video_id))

		return comments

	except HttpError as e:
		return comments
		raise


def bundle_video_ids(video_id_set, ids_per_string = 5):

	video_ids = []
	count = 0
	id_string = ""

	for id in video_id_set:
		id_string += id
		id_string += ","
		count += 1
		if(count >= ids_per_string):
			id_string = id_string[:-1]
			video_ids.append(id_string)
			id_string = ""
			count = 0

	if(count != 0):
		id_string = id_string[:-1]
		video_ids.append(id_string)

	return video_ids

def get_uploaded_ids(user_name):

	# We need to create the service
	service = build_service()

	# Get the channel object from the API
	channel_part = "contentDetails,statistics"
	channel = get_channel(user_name, channel_part, service)

	# Get all of the video IDs uploaded by the user
	results_per_page = 50
	playlist_id = channel['contentDetails']['relatedPlaylists']['uploads']
	playlist_size = int(channel['statistics']['videoCount'])
	playlist_part = "snippet"
	playlist_token = ""

	# Make sure no duplicate ids are added
	video_id_set = set()

	# We may need to make multiple requests
	page_count = math.ceil(playlist_size / results_per_page)

	# Get the video IDs from the API
	for index in range(page_count):

		playlist = get_video_ids(
			playlist_part = playlist_part, 
			playlist_id = playlist_id, 
			playlist_token = playlist_token, 
			results_per_request = results_per_page, 
			service = service)

		# Get the next page token if available
		if('nextPageToken' in playlist):
			playlist_token = playlist['nextPageToken']

		# Add each video ID to the set of video IDs
		for video in playlist['items']:
			id = video['snippet']['resourceId']['videoId']
			video_id_set.add(id)
	
	# Prepare to make the requests
	video_ids = bundle_video_ids(video_id_set, results_per_page)

	# Return the results
	return video_ids

def get_video_attributes(video):

	# We are interested in getting these video attributes
	views = -1
	comments = -1
	likes = -1
	dislikes = -1
	title = ""

	# Check if these attributes are enabled
	if('viewCount' in video['statistics']):
		views = video['statistics']['viewCount']

	if('commentCount' in video['statistics']):
		comments = video['statistics']['commentCount']

	if('likeCount' in video['statistics']):
		comments = video['statistics']['likeCount']

	if('dislikeCount' in video['statistics']):
		comments = video['statistics']['dislikeCount']

	if('title' in video['snippet']):
		title = video['snippet']['title']

	metadata_string = "{0}\t{1}\t{2}\t{3}\t{4}".format(
		views, comments, likes, dislikes, title)

	return metadata_string

def get_comment_attributes(comment, video_id):

	# Top-level comments do not have the 'parentID' member set
	parent_id = " "
	if('parentId' in comment['snippet']):
		parent_id = comment['snippet']['parentId']

	# TODO: Make sure to remove any tab characters in usernames as it was
	# discovered that users can have tabs in usernames,
	# also be sure to remove any \n and \r characters in usernames
	# as those can also be in usernames for some reason

	text = comment['snippet']['textDisplay'].replace("\r\n", " ")
	text = text.replace("\n\r", " ")
	text = text.replace("\r", " ")
	text = text.replace("\n", " ")

	comment_string = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}".format(
		comment['id'],
		parent_id,
		video_id,
		comment['snippet']['authorDisplayName'],
		comment['snippet']['likeCount'],
		text
	)

	return comment_string

def get_video_metadata(user_name = "MrBeast6000"):

	# Get the ids of all uploaded videos
	video_ids = get_uploaded_ids(user_name)

	video_part = "snippet,statistics"
	metadata = []

	for id_string in video_ids:

		videos = get_videos(video_part, id_string, service)

		for video in videos:
			metadata.append(get_video_attributes(video))

	return metadata

def get_comment_data(user_name = "MrBeast6000"):

	# Get the video ids uploaded by the given user
	video_ids = get_uploaded_ids(user_name)
	comments = []
	service = build_service()

	try:
		for id_string in video_ids:
			for id in id_string.split(","):
				print("Getting comments for video with video id " + id)
				comments += get_comments("id,snippet", id, service)
	except HttpError as e:
		print(e)
	finally:
		return comments

if __name__ == "__main__":
	get_video_metadata()