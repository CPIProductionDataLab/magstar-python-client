__version__ = "0.1.0"
__author__ = "Robert P. Cope"
__author_email__ = "rcope@cpi.com"

import requests
import time
from urllib.parse import urljoin
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Iterator
from datetime import datetime as DT
import pytz


@dataclass
class MagstarStationData(object):
    station_id: int
    name: str
    location: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    acronym: Optional[str]
    status: Optional[str]
    last_seen: Optional[float]

    @classmethod
    def from_api(cls, raw: Dict) -> 'MagstarStationData':
        return cls(
            raw["station_id"],
            raw["name"],
            raw["location"],
            raw["lat"],
            raw["lon"],
            raw["acronym"],
            raw["status"],
            raw["last_seen"]
        )


@dataclass
class MagstarStationExtendedData(object):
    station_id: int
    name: str
    location: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    acronym: Optional[str]
    status: Optional[str]
    last_seen: Optional[float]
    earliest_timestamp: Optional[float]
    latest_timestamp: Optional[float]
    latest_x: Optional[float]
    latest_y: Optional[float]
    latest_z: Optional[float]
    latest_horizontal_field_angle: Optional[float]
    latest_horizontal_field_magnitude: Optional[float]

    @classmethod
    def from_api(cls, raw: Dict) -> 'MagstarStationExtendedData':
        return cls(
            raw["station_id"],
            raw["name"],
            raw["location"],
            raw["lat"],
            raw["lon"],
            raw["acronym"],
            raw["status"],
            raw["last_seen"],
            raw["earliest_timestamp"],
            raw["latest_timestamp"],
            raw["latest_x"],
            raw["latest_y"],
            raw["latest_z"],
            raw["latest_horizontal_field_angle"],
            raw["latest_horizontal_field_magnitude"]
        )


@dataclass
class MagstarMeasurement(object):
    timestamp: float
    b_x: float
    b_y: float
    b_z: float
    temperature: float
    horizontal_field_angle: Optional[float]
    horizontal_field_magnitude: Optional[float]
    operator_config_hash: Optional[str]
    instrument_config_hash: Optional[str]

    @property
    def timestamp_datetime(self) -> DT:
        return DT.utcfromtimestamp(self.timestamp).replace(tzinfo=pytz.UTC)

    @classmethod
    def from_api(cls, raw: Dict) -> 'MagstarMeasurement':
        return cls(
            raw["timestamp"],
            raw["x"],
            raw["y"],
            raw["z"],
            raw["temperature"],
            raw["horizontal_field_angle"],
            raw["horizontal_field_magnitude"],
            raw["operator_config_hash"],
            raw["instrument_config_hash"]
        )


@dataclass
class MagstarMeasurementResult(object):
    measurements: List[MagstarMeasurement]
    has_further_data: bool
    next_ts: Optional[float]

    @classmethod
    def from_api(cls, raw: Dict) -> 'MagstarMeasurementResult':
        return cls(
            [MagstarMeasurement.from_api(m) for m in raw["measurements"]],
            raw["has_further_data"],
            raw["next_ts"]
        )


class MagstarV1API(object):
    """
    An API Wrapper for the Computational Physics Inc. Magstar V1 API

    :param base_url: The URL of the Magstar service (e.g. https://dasi.barlow.cpi.com)
    :type base_url: str
    :param api_key: The API key to use to retrieve data.
    :type api_key: str
    """
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.session()

    def _get(self, sub_url: str, params: Optional[Dict] = None) -> Any:
        result = self.session.get(
            urljoin(self.base_url, sub_url),
            params=params,
            headers={
                'X-API-Key': self.api_key,
                'User-Agent': 'MagstarV1API ({0})'.format(__version__)
            }
        )
        result.raise_for_status()
        return result.json()

    def get_stations(self) -> List[MagstarStationData]:
        """
        Retrieve a list of stations with their basic information.
        """
        return [MagstarStationData.from_api(r) for r in self._get("/v1/stations")]

    def get_station_details_by_id(self, station_id: int) -> MagstarStationExtendedData:
        """
        Retrieve detailed information about a station.
        """
        assert isinstance(station_id, int)
        return MagstarStationExtendedData.from_api(self._get("/v1/stations/{0}".format(station_id)))

    def get_station_measurements_by_id(self, station_id: int, after_ts: Optional[float] = None,
                                       before_ts: Optional[float] = None, limit: Optional[int] = None,
                                       reverse_order: bool = False) -> MagstarMeasurementResult:
        """
        Retrieve stored measurements for a given station ID.

        :param station_id: The station ID to fetch measurements for.
        :param after_ts: The minimum UTC Unix epoch (inclusive) to load measurements for.
        :param before_ts: The maximum UTC Unix epoch (inclusive) to load measurements for.
        :param limit: The maximum number of measurements to return for this API call.
          The greatest limit permissable is 2500.
        :param reverse_order: If True, measurements are loaded from newest to oldest, if false from oldest to newest.
        """
        assert isinstance(station_id, int)
        params = {
            'reverse_order': reverse_order
        }
        if after_ts is not None:
            params['after_ts'] = after_ts
        if before_ts is not None:
            params['before_ts'] = before_ts
        if limit is not None:
            params['limit'] = limit
        return MagstarMeasurementResult.from_api(self._get(
            "/v1/stations/{0}/measurements".format(station_id),
            params=params
        ))

    def iterate_station_measurements(self, station_id: int, after_ts: Optional[float] = None,
                                     before_ts: Optional[float] = None, reverse_order: bool = False,
                                     poll_delay: float = 0.1) -> Iterator[MagstarMeasurement]:
        """
        Retrieve stored measurements for a given station ID as an iterator, with
        pagination handled automatically.

        This creates an iterator of magstar measurement records.

        :param station_id: The station ID to fetch measurements for.
        :param after_ts: The minimum UTC Unix epoch (inclusive) to load measurements for.
        :param before_ts: The maximum UTC Unix epoch (inclusive) to load measurements for.
        :param reverse_order: If True, measurements are loaded from newest to oldest, if false from oldest to newest.
        :param poll_delay: The amount of time to wait between each successive API. This should be set to something
          reasonable like 0.1 or 0.2 seconds.
        """
        need_measurements = True
        next_after_ts = after_ts
        next_before_ts = before_ts
        poll_delay = max(poll_delay, 0.1)

        while need_measurements:
            result = self.get_station_measurements_by_id(
                station_id, after_ts=next_after_ts, before_ts=next_before_ts,
                limit=2500, reverse_order=reverse_order
            )
            for m in result.measurements:
                yield m
            need_measurements = result.has_further_data
            if result.has_further_data and reverse_order:
                next_before_ts = result.next_ts
            elif result.has_further_data:
                next_after_ts = result.next_ts
            time.sleep(poll_delay)
