# pylint: disable=too-few-public-methods, unused-private-member
import logging

# import sys
# import logstash


class LoggingEtl:
    """
    Centralizing logging into a class.
    """

    def __init__(self):
        """
        Start logging for class.
        """
        logging.basicConfig(
            filename=f"{self.__class__.__name__}.log",
            encoding="utf-8",
            level=logging.DEBUG,
            format="[DATA][%(levelname)s]%(filename)s:%(lineno)d %(asctime)s - %(message)s",
        )
        self.log = logging.LoggerAdapter(
            logging.getLogger(self.__class__.__name__),
            {"class": self.__class__.__name__},
        )

    # Need testing

    # def log_to_logstash(self, host, port):
    #     """
    #     Send logs to logstash.
    #     """
    #     self.log.addHandler(logstash.TCPLogstashHandler(host, port, version=1))

    # def log_to_stdout(self):
    #     """
    #     Send logs to stdout.
    #     """
    #     stdout_handler = logging.StreamHandler(sys.stdout)
    #     stdout_handler.setLevel(logging.DEBUG)
    #     stdout_handler.setFormatter(
    #         logging.Formatter(
    #             "[DATA][%(levelname)s]%(filename)s:%(lineno)d %(asctime)s - %(message)s"
    #         )
    #     )
    #     self.log.addHandler(stdout_handler)

    # def log_to_file(self):
    #     """
    #     Send logs to file.
    #     """
    #     file_handler = logging.FileHandler(f"{self.__class__.__name__}.log")
    #     file_handler.setLevel(logging.DEBUG)
    #     file_handler.setFormatter(
    #         logging.Formatter(
    #             "[DATA][%(levelname)s]%(filename)s:%(lineno)d %(asctime)s - %(message)s"
    #         )
    #     )
    #     self.log.addHandler(file_handler)

    # def log_to_fluentd(self, host, port):
    #     """
    #     Send logs to fluentd.
    #     """
    #     fluentd_handler = logstash.FluentHandler(host, port)
    #     fluentd_handler.setLevel(logging.DEBUG)
    #     fluentd_handler.setFormatter(
    #         logging.Formatter(
    #             "[DATA][%(levelname)s]%(filename)s:%(lineno)d %(asctime)s - %(message)s"
    #         )
    #     )
    #     self.log.addHandler(fluentd_handler)

    # def log_to_all(self, host, port):
    #     """
    #     Send logs to all.
    #     """
    #     self.log_to_logstash(host, port)
    #     self.log_to_stdout()
    #     self.log_to_file()
