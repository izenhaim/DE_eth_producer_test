class LogService:
	console_output = "console_output"
	file_output = "file_output"

	def __init__(self, is_logging_on=False, log_output=console_output, file_path=""):
		self.is_logging_on = is_logging_on
		self.log_output = log_output
		self.log_file_path = file_path

	def turn_logging_on(self):
		self.is_logging_on = True

	def turn_logging_off(self):
		self.is_logging_on = False

	def log(self, message):

		if self.is_logging_on:
			if self.log_output == self.console_output:
				print(str(message))
			elif self.log_output == self.file_output:
				with open(self.log_file_path, "a+") as file:
					file.write(str(message) + "\r\n")

	def set_log_output(self, output, file_path=""):
		self.log_output = output
		self.log_file_path = file_path
