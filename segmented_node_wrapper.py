from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ndn.encoding import Name
from ndn.schema import policy
from ndn.schema.simple_cache import MemoryCache, MemoryCachePolicy
from ndn.schema.simple_node import SegmentedNode


SEGMENT_SIZE = 4400


class SegmentedNodeWrapper:
    def __init__(
        self,
        app: Any,
        prefix: str,
        *,
        serve: bool = False,
    ) -> None:
        self._app = app
        self._prefix = Name.from_str(prefix)
        if not self._prefix:
            raise ValueError("prefix must not be the root name")
        self._serve = serve
        self._nodes: dict[str, SegmentedNode] = {}

    async def publish(
        self,
        func_name: str,
        content: bytes | bytearray | memoryview,
        *,
        freshness_period: int = 0,
        **kwargs: Any,
    ) -> str:
        if not self._serve:
            raise RuntimeError("publish() requires a wrapper created with serve=True")
        data = bytes(content)
        if not data:
            raise ValueError("content must not be empty")
        func_name = Name.from_str(func_name)
        published_name = self._make_published_name(func_name)
        node = await self._get_or_create_node(published_name)
        await node.match(published_name).provide(
            data, freshness_period=freshness_period, **kwargs
        )
        return Name.to_str(published_name)

    async def fetch(
        self,
        published_name: str,
        **kwargs: Any,
    ) -> bytes:
        content, _ = await self.fetch_with_metadata(published_name, **kwargs)
        return content

    async def fetch_with_metadata(
        self,
        published_name: str,
        **kwargs: Any,
    ) -> tuple[bytes, Mapping[str, Any]]:
        name = Name.from_str(published_name)
        self._retrieve_func_name(name)
        node = self._new_node()
        node.app = self._app
        node.prefix = name
        content, metadata = await node.match(name).need(**kwargs)
        return bytes(content), metadata

    def _make_published_name(self, func_name: list[bytes]) -> list[bytes]:
        if not func_name:
            raise ValueError("name must not be the root name")
        return self._prefix + func_name

    def _retrieve_func_name(self, published_name: list[bytes]) -> list[bytes]:
        if (len(published_name) <= len(self._prefix)
                or published_name[:len(self._prefix)] != self._prefix):
            raise ValueError("name must be below the prefix")
        return published_name[len(self._prefix):]

    async def _get_or_create_node(self, published_name: list[bytes]) -> SegmentedNode:
        name = Name.to_str(published_name)
        node = self._nodes.get(name)
        if node is None:
            node = self._new_node()
            node.prefix = published_name
            node.set_policy(policy.Cache, MemoryCachePolicy(MemoryCache()))
            if not await node.attach(self._app, published_name):
                raise RuntimeError("NFD rejected prefix registration")
            self._nodes[name] = node
        return node

    @staticmethod
    def _new_node() -> SegmentedNode:
        return SegmentedNode(
            timeout=4000,
            retry_times=3,
            segment_size=SEGMENT_SIZE,
        )

