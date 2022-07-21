import os
import sys
from pathlib import Path
from googleapiclient.errors import HttpError

import YouTubeAPI

def check_present_files(data_directory):

	path = Path(data_directory)
	return [file.stem for file in path.iterdir() if file.is_file()]

def parse_input(line):
	file_name, user_name = line.split(",")
	return file_name.strip(), user_name.strip()

def write_metadata(file_name, metadata, encoding="utf-8"):

	output_file = open(file = file_name, mode = "w", encoding = encoding)

	for line in metadata:
		print(line, file = output_file)

	output_file.close()

def run(input_file = ""):

	directory = "C:\\\\data\\youtube_api_data\\"
	error = "Error response status code: {0}, reason: {1}"
	
	# Make sure we only query for data we don't have
	files = check_present_files(directory)
	
	with open(file = input_file, mode = "r") as input:

		input_lines = input.readlines()
		for line in input_lines:

			# We can ignore lines that start with #, and any empty lines
			if(not line.startswith("#") and len(line) != 0):

				# Get the common name and user name for each YouTuber
				file_name, user_name = parse_input(line)

				# Make sure we haven't yet processed this YouTuber's
				# channel
				if(file_name not in files):

					try:
						metadata = YouTubeAPI.get_video_metadata(user_name)
						output_file = directory + file_name + ".txt"
						write_metadata(output_file, metadata, "UTF_16_BE")
					except HttpError as e:
						print("FAILED: <" + user_name + ">")
						print(error.format(e.resp.status, e.error_details))


if __name__ == "__main__":
	if(len(sys.argv) <= 1):
		run()
	else:
		run(sys.argv[1])