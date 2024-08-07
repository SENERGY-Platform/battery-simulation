"""
   Copyright 2022 InfAI (CC SES)

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

__all__ = ("Operator", )

from operator_lib.util import OperatorBase, logger, InitPhase, todatetime, timestamp_to_str
from operator_lib.util.persistence import save, load
import pandas as pd
import os
from time import sleep

from operator_lib.util import Config
class CustomConfig(Config):
    data_path = "/opt/data"
    capacity : float = 500
    max_charging_power : float = 1000
    max_discharging_power : float = 1000

    def __init__(self, d, **kwargs):
        super().__init__(d, **kwargs)

class Operator(OperatorBase):
    configType = CustomConfig

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        self.data_path = self.config.data_path

        self.capacity = self.config.capacity
        self.max_capacity = self.self.config.max_capacity
        self.max_charging_power = self.config.max_charging_power
        self.max_discharging_power = self.config.max_discharging_power

        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)

    def run(self, data, selector='energy_func', device_id=''):
        self.capacity = self.capacity + ((pd.Timestamp.now()-self.timestamp_control)/pd.Timedelta(hours=1))*self.battery_power

        if self.capacity >= self.max_capacity:
            self.capacity = self.max_capacity
        elif self.capacity < 0:
            self.capacity = 0

        self.timestamp_control = todatetime(data['Time']).tz_localize(None)
        self.battery_power = data['Power']

        if self.battery_power > self.max_charging_power:
            self.battery_power = self.max_charging_power
        elif self.battery_power < -self.max_discharging_power:
            self.battery_power = -self.max_discharging_power
        
        logger.debug('Battery Power: '+str(self.battery_power)+'  '+'time: '+str(self.timestamp_control))

        sleep(60)

        logger.debug(f"Next output: capacity is {self.capacity}       timestamp is around {pd.Timestamp.now()}")

        return {
                    "capacity": self.capacity,
                    "timestamp": timestamp_to_str(pd.Timestamp.now())
        }

from operator_lib.operator_lib import OperatorLib
if __name__ == "__main__":
    OperatorLib(Operator(), name="leakage-detection-operator", git_info_file='git_commit')

