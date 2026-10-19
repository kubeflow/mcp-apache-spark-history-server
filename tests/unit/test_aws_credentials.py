import subprocess
from unittest.mock import MagicMock

from botocore.credentials import ProcessProvider

from spark_history_mcp.utils.aws_credentials import isolate_credential_process_stdin


def _provider(popen):
    return ProcessProvider("default", load_config=lambda: {}, popen=popen)


def test_stdin_is_devnull():
    isolate_credential_process_stdin()
    popen = MagicMock()
    _provider(popen)._popen(["helper"], stdout=subprocess.PIPE)
    popen.assert_called_once_with(
        ["helper"], stdout=subprocess.PIPE, stdin=subprocess.DEVNULL
    )


def test_explicit_stdin_is_kept():
    isolate_credential_process_stdin()
    popen = MagicMock()
    _provider(popen)._popen(["helper"], stdin=subprocess.PIPE)
    popen.assert_called_once_with(["helper"], stdin=subprocess.PIPE)


def test_patch_is_applied_once():
    isolate_credential_process_stdin()
    patched = ProcessProvider.__init__
    isolate_credential_process_stdin()
    assert ProcessProvider.__init__ is patched
