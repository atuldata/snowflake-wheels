"""
Trap a list of signals and execute callback function.
"""

from itertools import chain
import signal
import sys

EXIT_SIGNALS = chain(range(1, 9), range(10, 17), [23, 24], range(26, 32))


class ExitSignalTrap(object):
    """
    Traps all known exit/kill/stop signals and calls the supplied callback.
    Attempts to ensure that a callback is called before exiting.
    """

    def __init__(self, callback, signals=EXIT_SIGNALS):
        self.callback = callback
        self._signals = signals

        for sig in signals:
            try:
                signal.signal(sig, self._signal_trap)
            except:
                # Not all systems recognize all of these signals.
                pass

    @property
    def signals(self):
        """
        Contructor argument as the signal handling is setup at init.
        This should be a list of integers representing signal codes.
        Default set is all known(at this time) exit signals except 9
        which is reserved for outright murder when needed.
        """
        return self._signals

    def _signal_trap(self, signal_no):
        """
        This is the callback for the signal trapping.
        It will call on self.callback()
        """
        sys.stderr.write(
            "Caught signal %s. Calling close() now...\n" % str(signal_no))
        self.callback()
        sys.exit()
