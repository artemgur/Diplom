import subprocess
import signal
import os


def start_subprocess(command: str, visible=False):
    """
    Starts subprocess, optionally in visible terminal window.
    :param command: command which will be executed
    :param visible: if True, then the command will execute in visible terminal window (currently only works on Windows).
    :return: running subprocess.
    """
    if visible:
        command = 'start /wait ' + command
    return subprocess.Popen(command, shell=True)


def terminate_shell_subprocess(process):
    """
    Terminates subprocess started with shell=True.

    Necessary, as process.kill() and process.terminate() don't work when subprocess was started with shell=True.
    """
    if os.name == 'nt':
        # Windows
        subprocess.Popen(f"TASKKILL /F /PID {process.pid} /T")
    else:
        # Linux (not tested)
        os.kill(process.pid, signal.SIGTERM)