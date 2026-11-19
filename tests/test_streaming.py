import json
import unittest

from src.streaming import classify


class StreamingValidationTests(unittest.TestCase):
    def setUp(self):
        self.event = {
            "appointment_id": "A1",
            "department": "Cardiology",
            "status": "completed",
            "lab_tests": "2",
            "patient_id": "P001",
        }

    def classify_event(self, **changes):
        return classify(json.dumps(self.event | changes).encode())

    def test_csv_count_is_normalized_and_extra_fields_are_dropped(self):
        self.assertEqual(self.classify_event(), ("A1", "Cardiology", "completed", 2, None))

    def test_all_supported_statuses_and_zero_counts(self):
        for status in ("completed", "cancelled", "scheduled"):
            with self.subTest(status=status):
                self.assertEqual(self.classify_event(status=status, lab_tests=0)[2:], (status, 0, None))

    def test_invalid_fields_leave_only_a_rejection_code(self):
        for changes, reason in [
            ({"appointment_id": " "}, "invalid_appointment_id"),
            ({"department": "a\x00b"}, "invalid_department"),
            ({"department": "\ud800"}, "invalid_department"),
            ({"status": ["completed"]}, "invalid_status"),
            ({"status": "unknown"}, "invalid_status"),
        ]:
            with self.subTest(changes=changes):
                self.assertEqual(self.classify_event(**changes), (None, None, None, None, reason))

    def test_invalid_counts(self):
        for count in (None, -1, True, 1.5, "1.5", 2_147_483_648, "２"):
            with self.subTest(count=count):
                self.assertEqual(self.classify_event(lab_tests=count)[-1], "invalid_lab_tests")

    def test_bad_messages_are_classified(self):
        for value in (b"{", b"\xff"):
            self.assertEqual(classify(value)[-1], "malformed_json")
        for value in (None, b"null", b"[]"):
            self.assertEqual(classify(value)[-1], "invalid_event")
