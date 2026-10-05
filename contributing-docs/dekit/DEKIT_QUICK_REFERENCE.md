<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Quick Reference: dekit Support in Breeze

[dekit](https://github.com/pvolok/mprocs) is the next version of mprocs under a new name; Breeze
used mprocs before. The `mprocs` name is still accepted as an alias of `dekit` wherever Breeze
takes a terminal multiplexer, and a remembered `mprocs` choice is switched to `dekit` automatically.

## Basic Command

Start airflow uses dekit by default:

```bash
breeze start-airflow
```

You can switch to using `tmux` instead by specifying the option:

```bash
breeze start-airflow --terminal-multiplexer tmux
```

Once you select the terminal multiplexer, Breeze will remember your choice for future runs.

You can also switch terminal multiplexers by running `breeze setup config`:

```bash
breeze setup config --terminal-multiplexer dekit
breeze setup config --terminal-multiplexer tmux
```

Breeze starts the Airflow components as dekit tasks, so they all run in a single terminal window.
It generates the `dekit.yaml` configuration from the selected executor and options; inside the
container it is in `/tmp/breeze-dekit/dekit.yaml`.

## Common Usage Patterns

| Command                                                        | Description                                        |
|----------------------------------------------------------------|----------------------------------------------------|
| `breeze start-airflow --terminal-multiplexer dekit`            | Start Airflow with dekit (and remember the choice) |
| `breeze start-airflow --terminal-multiplexer tmux`             | Start Airflow with tmux (and remember the choice)  |
| `breeze start-airflow --debug scheduler`                       | Debug scheduler with last selected multiplexer     |
| `breeze start-airflow --dev-mode --terminal-multiplexer dekit` | Use dekit in dev mode (and remember the choice)    |


## dekit Keyboard Shortcuts

| Key          | Action                                          |
|--------------|-------------------------------------------------|
| `↑↓` / `jk`  | Navigate between tasks                          |
| `r`          | Restart selected task                           |
| `x`          | Stop selected task                              |
| `s`          | Start selected task                             |
| `z`          | Zoom into the selected task                     |
| `Ctrl+a`     | Switch focus between the task list and terminal |
| `q`          | Leave dekit (ends the Breeze session)           |
| `Q`          | Stop all tasks and leave dekit                  |
| `?`          | Show all key bindings                           |

When stopping the Breeze environment, press `q` or `Q` rather than stopping only the
selected task. Leaving dekit lets the `breeze start-airflow` container exit
and release its forwarded ports. After returning to the host shell, run:

```bash
breeze down
```

## Components Managed

- **scheduler** - Airflow scheduler
- **api_server** (3.x+) / **webserver** (2.x) - Web interface
- **triggerer** - Handles deferred tasks
- **dag_processor** - Standalone Dag processor (when enabled)
- **celery_worker** - Celery worker (with CeleryExecutor)
- **flower** - Celery monitoring (when enabled)
- **edge_worker** - Edge worker (with EdgeExecutor)
- **shell** - A bash shell in the container

## Environment Variables

| Variable                   | Purpose                                          |
|----------------------------|--------------------------------------------------|
| `TERMINAL_MULTIPLEXER`     | `dekit` (default, `mprocs` also works) or `tmux` |
| `INTEGRATION_CELERY`       | Enable Celery components                         |
| `CELERY_FLOWER`            | Enable Flower UI                                 |
| `STANDALONE_DAG_PROCESSOR` | Enable standalone Dag processor                  |
| `BREEZE_DEBUG_*`           | Enable component debugging                       |
| `DEV_MODE`                 | Enable development mode                          |

## Debug Ports (when debugging enabled)

| Component     | Port   |
|---------------|--------|
| Scheduler     | 50231  |
| Dag Processor | 50232  |
| Triggerer     | 50233  |
| API Server    | 50234  |
| Celery Worker | 50235  |
| Edge Worker   | 50236  |
| Web Server    | 50237  |

## Installation

dekit is **pre-installed** in the Breeze CI image. No additional setup is required.

For custom setups, you can install it manually:

```bash
# Download and install dekit (Linux)
DEKIT_VERSION="0.10.0"
curl -L "https://github.com/pvolok/mprocs/releases/download/v${DEKIT_VERSION}/dekit-$(uname -m)-unknown-linux-musl.tar.gz" \
  | tar -xz -C /usr/local/bin/ dekit
chmod +x /usr/local/bin/dekit
```

## Mac OS X and iTerm2

Mouse clicks are not captured correctly by default in iTerm2 (which is often used by developers on MacOS).
You need to configure "Enable Mouse reporting" to take advantage of the copying feature and mouse handling:

![Enable mouse reporting](../images/iterm2-enable-mouse-reporting.png)

## Standalone execution

You can run dekit outside of Breeze for custom setups. Put a `dekit.yaml` defining your tasks in a
directory, then start the tasks and attach to them from that directory:

```bash
dekit up
dekit
```

`dekit up` starts every task marked `autostart: true`, and `dekit` attaches to them. The tasks keep
running when you leave with `q`; `dekit down` stops them.

An example [dekit.yaml](dekit.yaml) file for Airflow components can be found in this directory - it has
default configurations for all major Airflow components, but you can customize it as needed by copying it
elsewhere and modifying the task definitions, uncommenting the commented or adding new tasks. See
[Coming from mprocs](https://dekit.run/docs/start/from-mprocs) for how dekit differs from mprocs.
