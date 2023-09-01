# Copyright (C) 2012 Anaconda, Inc
# SPDX-License-Identifier: BSD-3-Clause
from ...base.context import context
from ...gateways.connection import Retry, HTTPAdapter
from .. import CondaTransportAdapter, hookimpl


@hookimpl
def conda_transport_adapters():
    # Configure retries
    retry = Retry(
        total=context.remote_max_retries,
        backoff_factor=context.remote_backoff_factor,
        status_forcelist=[413, 429, 500, 503],
        raise_on_status=False,
    )
    http_adapter = HTTPAdapter(max_retries=retry)
    yield CondaTransportAdapter(name="http", prefix="http://", adapter=http_adapter)
    yield CondaTransportAdapter(name="https", prefix="https://", adapter=http_adapter)
