import unittest

from spark_history_mcp.models.spark_types import ThreadStackTrace


class TestThreadStackTrace(unittest.TestCase):
    """Test ThreadStackTrace model handles missing fields from older Spark versions."""

    def test_full_response_spark4(self):
        """Spark 4.x returns all fields including synchronizers, monitors, priority."""
        data = {
            "threadId": 1,
            "threadName": "main",
            "threadState": "RUNNABLE",
            "stackTrace": None,
            "blockedByThreadId": None,
            "blockedByLock": "",
            "holdingLocks": [],
            "synchronizers": [
                "java.util.concurrent.locks.ReentrantLock$NonfairSync@abc"
            ],
            "monitors": ["java.lang.Object@def"],
            "lockName": None,
            "lockOwnerName": None,
            "suspended": False,
            "inNative": False,
            "isDaemon": True,
            "priority": 5,
        }
        trace = ThreadStackTrace.model_validate(data)
        self.assertEqual(trace.thread_id, 1)
        self.assertEqual(trace.thread_name, "main")
        self.assertEqual(len(trace.synchronizers), 1)
        self.assertEqual(len(trace.monitors), 1)
        self.assertFalse(trace.suspended)
        self.assertEqual(trace.priority, 5)
        self.assertTrue(trace.is_daemon)

    def test_minimal_response_spark3(self):
        """Spark 3.x may omit synchronizers, monitors, isDaemon, and priority."""
        data = {
            "threadId": 42,
            "threadName": "executor-thread-1",
            "threadState": "WAITING",
            "stackTrace": None,
            "blockedByThreadId": None,
            "blockedByLock": "",
            "holdingLocks": [],
            "lockName": None,
            "lockOwnerName": None,
            "inNative": False,
        }
        trace = ThreadStackTrace.model_validate(data)
        self.assertEqual(trace.thread_id, 42)
        self.assertEqual(trace.thread_name, "executor-thread-1")
        self.assertEqual(trace.synchronizers, [])
        self.assertEqual(trace.monitors, [])
        self.assertFalse(trace.suspended)
        self.assertEqual(trace.priority, 0)
        self.assertIsNone(trace.is_daemon)

    def test_partial_response(self):
        """Some fields present, some missing - mixed Spark version scenario."""
        data = {
            "threadId": 10,
            "threadName": "shuffle-server",
            "threadState": "TIMED_WAITING",
            "stackTrace": None,
            "blockedByThreadId": None,
            "blockedByLock": "",
            "holdingLocks": [],
            "synchronizers": [],
            "lockName": "java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@1234",
            "lockOwnerName": None,
            "suspended": True,
            "inNative": True,
            "isDaemon": False,
        }
        trace = ThreadStackTrace.model_validate(data)
        self.assertEqual(trace.priority, 0)
        self.assertTrue(trace.suspended)
        self.assertTrue(trace.in_native)
        self.assertFalse(trace.is_daemon)
        self.assertEqual(trace.synchronizers, [])


if __name__ == "__main__":
    unittest.main()
