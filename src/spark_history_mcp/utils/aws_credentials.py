"""Keep botocore credential_process helpers off the MCP stdio channel."""

import functools
import subprocess  # nosec B404 - only subprocess.DEVNULL; botocore starts the process

from botocore.credentials import ProcessProvider


def isolate_credential_process_stdin() -> None:
    """Start credential_process helpers with stdin from DEVNULL (idempotent)."""
    if getattr(ProcessProvider.__init__, "_stdin_isolated", False):
        return
    original_init = ProcessProvider.__init__

    @functools.wraps(original_init)
    def init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        popen = self._popen

        # botocore redirects only stdout and stderr, so the helper inherits
        # stdin, which is the MCP JSON-RPC pipe on stdio.
        def popen_without_stdin(*popen_args, **popen_kwargs):
            popen_kwargs.setdefault("stdin", subprocess.DEVNULL)
            return popen(*popen_args, **popen_kwargs)

        self._popen = popen_without_stdin

    init._stdin_isolated = True  # type: ignore[attr-defined]
    ProcessProvider.__init__ = init
