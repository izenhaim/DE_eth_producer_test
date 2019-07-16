class LogService:

	console_output = "console_output"
	file_output = "file_output"

	def __init__(self):
		self.is_logging_on = False
		self.log_output = self.console_output
		self.log_file_path = ""

	def turn_logging_on(self):
		self.is_logging_on = True

	def turn_logging_off(self):
		self.is_logging_on = False

	def log(self, message):
		# TODO
		if self.is_logging_on:
			print(str(message))

	def set_log_output(self, output, file_path=""):
		self.log_output = output
		self.log_file_path = file_path
