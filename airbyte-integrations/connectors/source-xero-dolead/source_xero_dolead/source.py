#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Dict, Callable, Union, Iterator

from airbyte_cdk.models import Type as MessageType
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.connector_state_manager import ConnectorStateManager
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.sources.utils.schema_helpers import split_config, InternalConfig
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.utils.event_timing import create_timer
from airbyte_protocol.models import SyncMode, ConfiguredAirbyteCatalog, AirbyteStateMessage, AirbyteMessage, FailureType, \
    AirbyteStreamStatus, StreamDescriptor, ConfiguredAirbyteStream
from airbyte_cdk.utils.stream_status_utils import as_airbyte_message as stream_status_as_airbyte_message


"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class XeroDoleadStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class XeroDoleadStream(HttpStream, ABC)` which is the current class
    `class Customers(XeroDoleadStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(XeroDoleadStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalXeroDoleadStream((XeroDoleadStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    # TODO: Fill in the url base. Required.
    url_base = "http://abilling01.prod.dld"
    xero_id = ""
    BODY_REQUEST_METHODS = ("POST", "PUT", "PATCH", "GET")
    dolead_id = "ed4cac23-e9fe-4e05-89d6-b5c9fc6d2a32"
    dolead_inc_id = "c1de2758-b8aa-45d7-8c01-163c3bbf8c39"
    dolead_uk_id = "bfa6caee-df20-41f8-a42b-8b73603159d7"
    dolead_dds_id = "8cfa6be1-0d88-4046-b9d4-e5055b0ade76"
    primary_key = ""

    @property
    def max_time(self) -> Union[int, None]:
        return None

    @property
    def max_retries(self) -> Union[int, None]:
        """
        Override if needed. Specifies maximum amount of retries for backoff policy. Return None for no limit.
        """
        return 10

    @property
    def retry_factor(self) -> float:
        return 5

    def next_page_token(self, response: requests.Response, **kwargs) -> int:
        decoded_response = response.json()
        if decoded_response.get("data"):
            last_object_page = decoded_response.get("page")
            return int(last_object_page) + 1
        else:
            return None

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        yield response.json()


class XeroPaginatedData(XeroDoleadStream):

    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_id, "page": str(next_page_token)}


class IncrementalXeroDoleadStream(XeroDoleadStream, IncrementalMixin, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    def __init__(self, *kwargs):
        super().__init__(*kwargs)
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value} if self._cursor_value else {}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return "JournalNumber"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        if latest_record and len(latest_record) > 0:
            try:
                latest_journal_number = latest_record["data"][len(latest_record["data"]) - 1]["JournalNumber"]
                return {"JournalNumber": latest_journal_number}
            except IndexError as e:
                state = self.state
                return state
        else:
            state = self.state
            return state

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:

        if next_page_token:
            return {"tenant_id": self.dolead_id, "offset": next_page_token}
        else:
            return {"tenant_id": self.dolead_id, "offset": self.state}


class Journals(IncrementalXeroDoleadStream):

    def path(self, **kwargs) -> str:
        return "/xero/journals"

    def next_page_token(self, response: requests.Response, **kwargs) -> int:
        decoded_response = response.json()
        if decoded_response.get("data"):
            # Get the last "JournalNumber" from the batch
            last_object_number = decoded_response.get("data")[len(decoded_response.get("data")) - 1].get("JournalNumber")
            return str(last_object_number)
        else:
            return None

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        return {"tenant_id": self.dolead_id, "offset": "330000"}


class DoleadJournals(Journals):
    dolead_id = "ed4cac23-e9fe-4e05-89d6-b5c9fc6d2a32"

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:

        if next_page_token:
            return {"tenant_id": self.dolead_id, "offset": next_page_token}
        else :
            try:
                request_body = {"tenant_id": self.dolead_id, "offset": int(self.state[self.cursor_field])}
            except KeyError as e:
                request_body = {"tenant_id": self.dolead_id, "offset": 0}
            return request_body


class TrackingCategories(XeroDoleadStream):

    def next_page_token(self, response: requests.Response, **kwargs) -> int:
        return None

    def path(self, **kwargs) -> str:
        return "/xero/tracking_categories"


class DoleadManualJournals(XeroPaginatedData):

    def path(self, **kwargs) -> str:
        return "/xero/manualjournals"

    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_id, "page": "0"}


class DoleadIncManualJournals(DoleadManualJournals):
    dolead_id = "c1de2758-b8aa-45d7-8c01-163c3bbf8c39"


class DoleadUkManualJournals(DoleadManualJournals):
    dolead_id = "bfa6caee-df20-41f8-a42b-8b73603159d7"


class DoleadDdsManualJournals(DoleadManualJournals):
    dolead_id = "8cfa6be1-0d88-4046-b9d4-e5055b0ade76"


class DoleadIncJournals(DoleadJournals):
    dolead_id = "c1de2758-b8aa-45d7-8c01-163c3bbf8c39"


class DoleadUkJournals(DoleadJournals):
    dolead_id = "bfa6caee-df20-41f8-a42b-8b73603159d7"


class DoleadDdsJournals(DoleadJournals):
    dolead_id = "8cfa6be1-0d88-4046-b9d4-e5055b0ade76"


class Contacts(XeroDoleadStream):

    def next_page_token(self, response: requests.Response, **kwargs) -> int:
        return None

    def path(self, **kwargs) -> str:
        return "/xero/all_contacts"


class Accounts(XeroDoleadStream):

    def next_page_token(self, response: requests.Response, **kwargs) -> int:
        return None

    def path(self, **kwargs) -> str:
        return "/xero/accounts"


class DoleadInvoices(XeroPaginatedData):
    def path(self, **kwargs) -> str:
        return "/xero/all_invoices"

    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_id, "page": "1"}


class DoleadIncInvoices(DoleadInvoices):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_inc_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_inc_id, "page": "1"}


class DoleadUkInvoices(DoleadInvoices):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_uk_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_uk_id, "page": "1"}


class DoleadDdsInvoices(DoleadInvoices):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_dds_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_dds_id, "page": "1"}


class DoleadCreditNotes(XeroPaginatedData):
    def path(self, **kwargs) -> str:
        return "/xero/credit_notes"

    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_id, "page": "1"}


class DoleadIncCreditNotes(DoleadCreditNotes):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_inc_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_inc_id, "page": "1"}


class DoleadUkCreditNotes(DoleadCreditNotes):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_uk_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_uk_id, "page": "1"}


class DoleadDdsCreditNotes(DoleadCreditNotes):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_dds_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_dds_id, "page": "1"}


class DoleadBankTransactions(XeroPaginatedData):
    def path(self, **kwargs) -> str:
        return "/xero/bank_transactions"

    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_id, "page": "1"}


class DoleadIncBankTransactions(DoleadBankTransactions):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_inc_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_inc_id, "page": "1"}


class DoleadUkBankTransactions(DoleadBankTransactions):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_uk_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_uk_id, "page": "1"}


class DoleadDdsBankTransactions(DoleadBankTransactions):
    def request_body_json(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> Dict:
        if next_page_token:
            return {"tenant_id": self.dolead_dds_id, "page": str(next_page_token)}
        else:
            return {"tenant_id": self.dolead_dds_id, "page": "1"}


class Employees(IncrementalXeroDoleadStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


# Source
class SourceXeroDolead(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        response = requests.get(url="http://abilling01.prod.dld/monitoring/version")
        if response.status_code == 200:
            return True, None
        else:
            return False, f"The API endpoint is unreachable. Please check your API."

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [Accounts(),
                DoleadManualJournals(), DoleadIncManualJournals(), DoleadUkManualJournals(), DoleadDdsManualJournals(),
                DoleadJournals(), DoleadIncJournals(), DoleadUkJournals(), DoleadDdsJournals(),
                Contacts(),
                DoleadInvoices(), DoleadIncInvoices(), DoleadUkInvoices(), DoleadDdsInvoices(),
                TrackingCategories(),
                DoleadCreditNotes(), DoleadIncCreditNotes(), DoleadUkCreditNotes(), DoleadDdsCreditNotes(),
                DoleadBankTransactions(), DoleadIncBankTransactions(), DoleadUkBankTransactions(), DoleadDdsBankTransactions()]
