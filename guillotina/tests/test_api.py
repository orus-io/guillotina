# -*- coding: utf-8 -*-
from guillotina import schema
from guillotina.interfaces import IApplication
from guillotina.testing import GuillotinaFunctionalTestCase
from guillotina.tests import TEST_RESOURCES_DIR
from guillotina.tests import utils
from zope.component import getUtility
from zope.interface import Interface

import json
import os


class ITestingRegistry(Interface):
    enabled = schema.Bool(
        title="Example attribute")


async def test_get_root(site_requester):
    async for requester in site_requester:
        response, status = await requester('GET', '/')
        assert response['static_file'] == ['favicon.ico']
        assert response['databases'] == ['guillotina']
        assert response['static_directory'] == []


async def test_get_database(site_requester):
    """Get the database object."""
    async for requester in site_requester:
        response, status = await requester('GET', '/guillotina')
        len(response['sites']) == 1


async def test_get_guillotina(site_requester):
    """Get the root guillotina site."""
    async for requester in site_requester:
        response, status = await requester('GET', '/guillotina/guillotina')
        assert len(response['items']) == 0


async def test_get_contenttypes(site_requester):
    """Check list of content types."""
    async for requester in site_requester:
        response, status = await requester('GET', '/guillotina/guillotina/@types')
        assert status == 200
        assert len(response) > 1
        assert any("Item" in s['title'] for s in response)
        assert any("Site" in s['title'] for s in response)


async def test_get_contenttype(site_requester):
    """Get a content type definition."""
    async for requester in site_requester:
        response, status = await requester('GET', '/guillotina/guillotina/@types/Item')
        assert status == 200
        assert len(response['definitions']) == 1
        assert response['title'] == 'Item'


async def test_get_registries(site_requester):
    """Get the list of registries."""
    async for requester in site_requester:
        response, status = await requester('GET', '/guillotina/guillotina/@registry')
        assert status == 200
        assert len(response['value']) == 2
        assert 'guillotina.interfaces.registry.ILayers.active_layers' in response['value']


async def test_get_registry_value(site_requester):
    """Check a value from registry."""
    async for requester in site_requester:
        response, status = await requester(
            'GET',
            '/guillotina/guillotina/@registry/guillotina.interfaces.registry.ILayers.active_layers')
        assert response['value'] == ['guillotina.interfaces.layer.IDefaultLayer']


async def test_create_contenttype(site_requester):
    async for requester in site_requester:
        response, status = await requester(
            'POST',
            '/guillotina/guillotina/',
            data=json.dumps({
                "@type": "Item",
                "title": "Item1",
                "id": "item1"
            })
        )
        assert status == 201
        request = utils.get_mocked_request(requester.db)
        root = await utils.get_root(request)
        site = await root.__getitem__('guillotina')
        obj = await site.__getitem__('item1')
        assert obj.title == 'Item1'


async def test_create_delete_contenttype(site_requester):
    """Create and delete a content type."""
    async for requester in site_requester:
        response, status = await requester(
            'POST',
            '/guillotina/guillotina/',
            data=json.dumps({
                "@type": "Item",
                "title": "Item1",
                "id": "item1"
            })
        )
        assert status == 201
        response, status = await requester('DELETE', '/guillotina/guillotina/item1')
        assert status == 200


async def test_register_registry(site_requester):
    async for requester in site_requester:
        response, status = await requester(
            'POST',
            '/guillotina/guillotina/@registry',
            data=json.dumps({
                "interface": "guillotina.tests.test_api.ITestingRegistry",
                "initial_values": {
                    "enabled": True
                }
            })
        )
        assert status == 201

        response, status = await requester(
            'PATCH',
            '/guillotina/guillotina/@registry/guillotina.tests.test_api.ITestingRegistry.enabled',
            data=json.dumps({
                "value": False
            })
        )
        assert status == 204
        response, status = await requester(
            'GET',
            '/guillotina/guillotina/@registry/guillotina.tests.test_api.ITestingRegistry.enabled')
        assert {'value': False} == response


async def test_create_contenttype_with_date(site_requester):
    async for requester in site_requester:
        response, status = await requester(
            'POST',
            '/guillotina/guillotina/',
            data=json.dumps({
                "@type": "Item",
                "title": "Item1",
                "id": "item1",
            })
        )
        assert status == 201
        date_to_test = "2016-11-30T14:39:07.394273+01:00"
        response, status = await requester(
            'PATCH',
            '/guillotina/guillotina/item1',
            data=json.dumps({
                "guillotina.behaviors.dublincore.IDublinCore": {
                    "created": date_to_test,
                    "expires": date_to_test
                }
            })
        )

        request = utils.get_mocked_request(requester.db)
        root = await utils.get_root(request)
        site = await root.__getitem__('guillotina')
        obj = await site.__getitem__('item1')
        from guillotina.behaviors.dublincore import IDublinCore
        assert IDublinCore(obj).created.isoformat() == date_to_test
        assert IDublinCore(obj).expires.isoformat() == date_to_test


async def test_create_duplicate_id(site_requester):
    async for requester in site_requester:
        response, status = await requester(
            'POST',
            '/guillotina/guillotina/',
            data=json.dumps({
                "@type": "Item",
                "title": "Item1",
                "id": "item1",
            })
        )
        assert status == 201
        response, status = await requester(
            'POST',
            '/guillotina/guillotina/',
            data=json.dumps({
                "@type": "Item",
                "title": "Item1",
                "id": "item1",
            })
        )
        assert status == 409
        response, status = await requester(
            'POST',
            '/guillotina/guillotina/',
            data=json.dumps({
                "@type": "Item",
                "title": "Item1",
                "id": "item1",
            }),
            headers={
                "OVERWRITE": "TRUE"
            }
        )
        assert status == 201


async def test_create_nested_object(site_requester):
    async for requester in site_requester:
        response, status = await requester(
            'POST',
            '/guillotina/guillotina/',
            data=json.dumps({
                '@type': 'Example',
                'title': 'Item1',
                'id': 'item1',
                'categories': [{
                    'label': 'term1',
                    'number': 1.0
                }, {
                    'label': 'term2',
                    'number': 2.0
                }]
            })
        )
        assert status == 201


async def test_get_addons(site_requester):
    async for requester in site_requester:
        response, status = await requester(
            'GET', '/guillotina/guillotina/@addons'
        )
        assert status == 200
