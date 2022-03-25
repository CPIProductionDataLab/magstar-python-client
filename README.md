# magstar-python-client

A Python client library for Computational Physics Inc.'s Magstar magnetometer networks.

## Installing

Either install directly from git:

```
git clone https://github.com/CPIProductionDataLab/magstar-python-client.git
cd magstar-python-client
python setup.py install
```

or install from PyPI:

```
pip install magstar-client
```

## Usage

```python

from magstar_client import MagstarV1API

BASE_URL = "???"
YOUR_API_KEY = "???"

api = MagstarV1API(BASE_URL, YOUR_API_KEY)
print(api.get_stations())
```

