# segmented-node-wrapper

A lightweight wrapper for segmented NDN data transfer using python-ndn's [`SegmentedNode`](https://python-ndn.readthedocs.io/en/v0.5.2/src/schema/schema_tree.html#ndn.schema.schema_tree).

It publishes a byte object as multiple Data packets and fetches it back as one `bytes` value. A serving instance registers its prefix; a fetch-only instance does not register the remote producer's prefix.

## Install

```bash
pip install "git+https://github.com/nakazato-lab/segmented-node-wrapper.git"
```

## Usage

The Manager publishes code and sends the returned name (`published_name`) in a small control Interest.

```python
import json
from pathlib import Path

from ndn.app import NDNApp
from segmented_node_wrapper import SegmentedNodeWrapper

app = NDNApp()
func = SegmentedNodeWrapper(app, "/manager/functions", serve=True)

async def main():
    function_name = "long-func"
    source_code = Path("long-func.py").read_bytes()
    published_name = await func.publish(function_name, source_code)
    request = {
        "type": "CREATE",
        "name": function_name,
        "runtime": "python",
        "code_ref": published_name,
    }
    await app.express_interest(
        "/node1/seed",
        app_param=json.dumps(request).encode(),
        must_be_fresh=True,
        can_be_prefix=True,
        lifetime=90000,
    )

app.run_forever(after_start=main())
```

`publish()` splits `source_code` into Data packets before the CREATE Interest
is sent. For example, the published object may be available as:

```text
/manager/functions/long-func/seg=0
/manager/functions/long-func/seg=1
/manager/functions/long-func/seg=2
```

The CREATE Interest sent to `/node1/seed` contains only the small `code_ref`
field, whose `published_name` value is `/manager/functions/long-func`.

The Seed fetches and reassembles the code with the received `published_name`.

```python
import asyncio
import json

from segmented_node_wrapper import SegmentedNodeWrapper

@app.route("/node1/seed")
def on_interest(_name, _param, app_param):
    if app_param is None:
        return
    request = json.loads(app_param)
    asyncio.create_task(handle_create(request["code_ref"]))

async def handle_create(published_name: str):
    func = SegmentedNodeWrapper(app, "/manager/functions", serve=False)
    source_code = await func.fetch(published_name)

app.run_forever()
```

`fetch()` sends Interests for each segment and returns the reassembled bytes:

```text
Seed -> Interest /manager/functions/long-func/seg=0 -> Manager
Seed -> Interest /manager/functions/long-func/seg=1 -> Manager
Seed -> Interest /manager/functions/long-func/seg=2 -> Manager
```

