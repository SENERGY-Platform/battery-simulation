# Battery Simulation

Simulates a battery for the [peak shaving operator](https://github.com/SENERGY-Platform/peak-shaving). Each control
of the peak shaving operator (`trigger_battery` = `yes`, once a minute) charges or discharges the battery with the
power of the previous control for the time since, and sets the power until the next one. The capacity goes back to
the peak shaving operator, which decides with it.

The power is limited to `max_charging_power` and `max_discharging_power`, and the capacity to between 0 and
`max_capacity`. A battery within 10 Wh of full is not charged, one within 10 Wh of empty not discharged.

Times are UTC: the time of a control is the time the platform received it. The capacity, the power and the time of
the last control are kept on the volume, so a restart continues with the battery as it was; `capacity` only sets
the capacity on a new volume.

## Input

| key             | type   | description                                                        |
|-----------------|--------|--------------------------------------------------------------------|
| Power           | float  | power to charge (> 0) or discharge (< 0) the battery with, in W    |
| trigger_battery | string | `yes` for a control; other messages are ignored                    |

## Output

| key      | type   | description                                                      |
|----------|--------|------------------------------------------------------------------|
| Capacity | float  | the capacity of the battery at the control, in Wh                |
| Time     | string | the time of the control, UTC, `%Y-%m-%dT%H:%M:%S.%fZ`             |

## Config options

| key                   | type   | default | description                                  |
|-----------------------|--------|---------|----------------------------------------------|
| capacity              | float  | 500     | capacity at the start on a new volume, in Wh |
| max_capacity          | float  | 500     | capacity of the full battery, in Wh          |
| max_charging_power    | float  | 1000    | highest charging power, in W                 |
| max_discharging_power | float  | 1000    | highest discharging power, in W              |
| logger_level          | string | warning | log level                                    |

## Development

`operator-manifest.json` is generated from the declarations in `main.py` with
`python -m operator_lib manifest main.py`; the CI regenerates it before each release. Tests:
`python -m unittest discover -s tests`.
