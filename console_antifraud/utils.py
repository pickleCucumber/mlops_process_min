import sys
import inspect
import ntpath
import datetime


class ConsoleLogger:
    def info(self, msg):
        print(msg)

    def warn(self, msg):
        print('WARN: ' + msg)

    def error(self, msg):
        print('ERROR: ' + msg)

    def debug(self, msg):
        print('DEBUG: ' + msg)

    def exception(self, msg):
        print('EXCEPTION: ' + msg)


class LoggerWrap(object):
    _logger = ConsoleLogger()

    def __get_caller_str_full(self, skip=0):
        """
        определение того, кто вызвал метод
        """
        caller = ''
        try:
            # вызываемый стек
            stack = inspect.stack()
            skip_to_caller = 2
            stack_obj = stack[skip_to_caller + skip][0]

            # вызываем объекты
            if 'self' in stack_obj.f_locals:
                # call from a class
                calling_object = stack_obj.f_locals['self'].__class__.__name__
            else:
                # call from a module
                filepath = ntpath.normpath(stack_obj.f_code.co_filename).split('\\')
                filename = '/'.join(filepath[-2:])
                calling_object = '[{}]'.format(filename)

            # вызываем функцию
            calling_function = stack_obj.f_code.co_name

            dt_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            caller = '{} {}.{}: '.format(dt_str, calling_object, calling_function)
        except:
            pass
        return caller

    def info(self, msg, skip=0):
        caller = self.__get_caller_str_full(skip)
        self._logger.info(caller + msg)

    def warn(self, msg, skip=0):
        caller = self.__get_caller_str_full(skip)
        self._logger.warn(caller + msg)

    def error(self, msg, skip=0):
        caller = self.__get_caller_str_full(skip)
        self._logger.error(caller + msg)

    def debug(self, msg, skip=0):
        caller = self.__get_caller_str_full(skip)
        self._logger.debug(caller + msg)

    def exception(self, msg, skip=0):
        caller = self.__get_caller_str_full(skip)
        self._logger.exception(caller + msg)


logger = LoggerWrap()


def mlog(msg):
    logger.info(msg, skip=1)