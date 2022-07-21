import os
import sys
from pathlib import Path
from googleapiclient.errors import HttpError

import YouTubeAPI



def run(input_file = ""):

	directory = "C:\\\\data\\youtube_comment_data\\"
	error = "Error response status code: {0}, reason: {1}"

	"""
		We need to make sure we keep track of which videos we have gotten all comments for
	"""

	# Test using Mr Beast (MrBeast6000)
	comments = YouTubeAPI.get_comment_data()
	file_name = directory + "MrBeast" + ".txt"

	output_file = open(file = file_name, mode = "w", encoding = "utf-8")

	for comment in comments:
		print(comment, file = output_file)

	output_file.close()


if __name__ == "__main__":
	if(len(sys.argv) <= 1):
		run()
	else:
		run(sys.argv[1])