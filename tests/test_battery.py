"""
   Copyright 2026 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import datetime
import tempfile
import unittest

from main import CustomConfig, Operator

UTC = datetime.timezone.utc


def at(hour, minute=0, day=1, month=6):
    # the time the platform received the message, as the library hands it over
    return datetime.datetime(2026, month, day, hour, minute, tzinfo=UTC)


class TestBattery(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.dir.cleanup()

    def start(self, **config):
        operator = Operator()
        operator.produce = lambda value: None
        operator.data_path = self.dir.name
        values = {"capacity": "500", "max_capacity": "1000", "max_charging_power": "1000",
                  "max_discharging_power": "1000"}
        values.update({key: str(value) for key, value in config.items()})
        operator.init(None, None, None, None, None, None, config=CustomConfig(values))
        return operator

    def control(self, operator, time, power, trigger="yes"):
        return operator.run({"Power": power, "trigger_battery": trigger}, None, "device", time)

    def test_charges_with_the_power_set_at_the_last_control(self):
        operator = self.start()
        self.assertEqual(500, self.control(operator, at(10), 600)["Capacity"])
        self.assertEqual(800, self.control(operator, at(10, 30), 0)["Capacity"])

    def test_discharges_with_a_negative_power(self):
        operator = self.start()
        self.control(operator, at(10), -400)
        self.assertEqual(300, self.control(operator, at(10, 30), 0)["Capacity"])

    def test_ignores_messages_that_do_not_trigger_the_battery(self):
        operator = self.start()
        self.control(operator, at(10), 600)
        self.assertIsNone(self.control(operator, at(10, 15), 0, trigger="no"))
        self.assertEqual(800, self.control(operator, at(10, 30), 0)["Capacity"])

    def test_stops_charging_at_the_maximum_capacity(self):
        operator = self.start()
        self.control(operator, at(10), 1000)
        self.assertEqual(1000, self.control(operator, at(11), 1000)["Capacity"])
        # full: the power is set to 0, so the capacity stays
        self.assertEqual(1000, self.control(operator, at(12), 0)["Capacity"])

    def test_stops_discharging_when_empty(self):
        operator = self.start()
        self.control(operator, at(10), -1000)
        self.assertEqual(0, self.control(operator, at(11), -1000)["Capacity"])
        self.assertEqual(0, self.control(operator, at(12), 0)["Capacity"])

    def test_limits_the_power_to_the_maximum_charging_and_discharging_power(self):
        operator = self.start(max_charging_power=200, max_discharging_power=100)
        self.control(operator, at(10), 600)
        self.assertEqual(700, self.control(operator, at(11), -600)["Capacity"])
        self.assertEqual(600, self.control(operator, at(12), 0)["Capacity"])

    def test_does_not_charge_a_nearly_full_battery(self):
        operator = self.start(capacity=995)
        self.control(operator, at(10), 500)
        self.assertEqual(995, self.control(operator, at(11), 0)["Capacity"])

    def test_does_not_discharge_a_nearly_empty_battery(self):
        operator = self.start(capacity=5)
        self.control(operator, at(10), -500)
        self.assertEqual(5, self.control(operator, at(11), 0)["Capacity"])

    def test_returns_the_time_of_the_control_in_utc(self):
        operator = self.start()
        self.assertEqual("2026-06-01T10:00:00.000Z", self.control(operator, at(10), 0)["Time"])

    def test_counts_the_real_duration_when_the_clocks_go_back(self):
        # On October 25, 2026 at 01:00 UTC the clocks in Germany go back from 03:00 to 02:00:
        # 00:30 and 01:30 UTC are both 02:30 there, but an hour apart.
        operator = self.start()
        self.control(operator, at(0, 30, day=25, month=10), 200)
        self.assertEqual(700, self.control(operator, at(1, 30, day=25, month=10), 0)["Capacity"])

    def test_starts_with_the_configured_capacity_on_a_new_volume(self):
        operator = self.start(capacity=250)
        self.assertEqual(250, self.control(operator, at(10), 0)["Capacity"])


if __name__ == '__main__':
    unittest.main()
