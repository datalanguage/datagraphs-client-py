import pytest
from datagraphs.client import HttpStatusError
from datagraphs.enums import ISOLATION_MODE
from lib import get_client, get_data, get_datasets, get_empty_schema, get_schema


class TestClient:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self):
        self.client = get_client('integration-testing')
        self.client.tear_down() 
        self.client.apply_schema(get_empty_schema())

    def test_should_verify_service_status(self) -> None:
        assert self.client.status() == 'OK'

    def test_should_apply_schema(self) -> None:
        schema = get_schema()
        self.client.apply_schema(schema)
        schema = self.client.get_schema()
        assert len(schema.classes) == 2

    def test_should_apply_datasets(self) -> None:
        schema = get_schema()
        self.client.apply_schema(schema)
        datasets = get_datasets()
        self.client.apply_datasets(datasets)

class TestDataOperations:

    @pytest.fixture(scope="class",autouse=True)
    def setup(self, request):
        request.cls.client = get_client('integration-testing')
        request.cls.client.tear_down(drop_datasets=False) 
        request.cls.client.apply_schema(get_schema())
        request.cls.client.apply_datasets(get_datasets())
        request.cls.client.put('pets', get_data())

    def test_should_create_data(self) -> None:
        assert len(self.client.get(class_name='Cat')) == 2
        assert len(self.client.get(class_name='Dog')) == 2

    def test_should_query_by_dataset(self) -> None:
        results = self.client.query(dataset='pets')
        assert len(results) == 4

    def test_should_query_by_search_phrase(self) -> None:
        results = self.client.query(q='Henry')
        assert len(results) == 1
        assert results[0]['name']['@none'] == 'Henry'

    def test_should_query_with_type_filter(self) -> None:
        results = self.client.query(filters='type:Cat')
        assert len(results) == 2
        names = ['Henry', 'Felix']
        assert results[0]['name']['@none'] in names
        assert results[1]['name']['@none'] in ['Henry', 'Felix']

    def test_should_query_with_facets(self) -> None:
        results, facets = self.client.query(facets='type')
        assert len(results) == 4 
        assert facets[0]['buckets'][0]['key'] == 'Cat'
        assert facets[0]['buckets'][0]['count'] == 2
        assert facets[0]['buckets'][1]['key'] == 'Dog'
        assert facets[0]['buckets'][1]['count'] == 2

    def test_should_query_with_custom_facet_size(self) -> None:
        results, facets = self.client.query(facets='type', facet_size=1)
        assert len(results) == 4
        assert facets[0]['buckets'][0]['key'] == 'Cat'
        assert facets[0]['buckets'][0]['count'] == 2
        assert len(facets[0]['buckets']) == 1

    def test_should_query_with_date_facets(self) -> None:
        results, facets = self.client.query(date_facets='_createdDate:1d:1w:1M')
        assert len(results) == 4
        assert facets[0]['buckets'][0]['key'] == '1d'
        assert facets[0]['buckets'][0]['count'] == 4
        assert len(facets[0]['buckets']) == 1

    def test_should_query_with_fields(self) -> None:
        results = self.client.query(fields='name')
        assert len(results) == 4
        for r in results:
            assert 'name' in r and 'nickname' not in r            

    def test_should_query_with_embed(self) -> None:
        results = self.client.query(embed='1')
        assert len(results) == 4

    def test_should_query_with_sort(self) -> None:
        results = self.client.query(sort='nickname:asc')
        assert len(results) == 4
        assert results[0]['name']['@none'] == 'Terminator'
        assert results[3]['name']['@none'] == 'Spotty'

    def test_should_query_with_sort_descending(self) -> None:
        results = self.client.query(dataset='pets', sort='nickname:desc')
        assert len(results) == 4
        assert results[0]['name']['@none'] == 'Spotty'
        assert results[3]['name']['@none'] == 'Terminator'

    def test_should_query_by_ids(self) -> None:
        all_results = self.client.query(dataset='pets')
        first_id = all_results[0]['id']
        results = self.client.query(ids=first_id)
        assert len(results) == 1
        assert results[0]['id'] == first_id

    def test_should_query_with_lang(self) -> None:
        results = self.client.query(q='Henry', lang='de')
        assert len(results) == 1
        assert results[0]['name'] == 'Heinrich'

    def test_should_query_with_page_size(self) -> None:
        results = self.client.query(page_size=1)
        assert len(results) == 1

    def test_should_query_with_page_no(self) -> None:
        page1 = self.client.query(page_size=1, page_no=1)
        page2 = self.client.query(page_size=1, page_no=2)
        assert len(page1) == 1
        assert len(page2) == 1
        assert page1[0]['id'] != page2[0]['id']

    def test_should_query_with_include_date_fields(self) -> None:
        results = self.client.query(include_date_fields=True)
        assert len(results) == 4
        for r in results:
            assert '_createdDate' in r and '_lastModifiedDate' in r

    def test_should_query_with_combined_parameters(self) -> None:
        results = self.client.query(
            q='Terminator',
            filters='type:Dog',
            fields='nickname',
            sort='label:asc',
            page_size=10,
            include_date_fields=True
        )
        assert len(results) == 1
        assert results[0]['nickname'] == 'Arnie'

    def test_should_return_query_page_with_total_and_next_page_token(self) -> None:
        page = self.client.query_page(filters='type:Cat', page_size=1)
        assert page.total_results == 2
        assert len(page.results) == 1
        assert page.next_page_token

    def test_should_fetch_next_page_by_token(self) -> None:
        page1 = self.client.query_page(filters='type:Cat', page_size=1)
        page2 = self.client.query_page(filters='type:Cat', page_size=1, next_page_token=page1.next_page_token)
        assert page2.total_results == 2
        assert {page1.results[0]['id'], page2.results[0]['id']} == {r['id'] for r in self.client.get(class_name='Cat')}

    def test_should_get_the_same_page_again_from_a_reused_token(self) -> None:
        # Retrying a failed page relies on a token being reusable.
        page1 = self.client.query_page(filters='type:Cat', page_size=1)
        first = self.client.query_page(filters='type:Cat', page_size=1, next_page_token=page1.next_page_token)
        again = self.client.query_page(filters='type:Cat', page_size=1, next_page_token=page1.next_page_token)
        assert first.results == again.results

    def test_should_read_every_page_of_a_query(self) -> None:
        results = self.client.query_all(dataset='pets', page_size=1)
        assert len(results) == 4
        assert len({r['id'] for r in results}) == 4

    def test_should_get_every_entity_of_a_class_across_pages(self) -> None:
        client = get_client('integration-testing', batch_size=1)
        dogs = client.get(class_name='Dog')
        assert len(dogs) == 2
        assert len({d['id'] for d in dogs}) == 2

    def test_should_run_gql_query(self) -> None:
        rows = self.client.query(gql='MATCH (n:Cat) RETURN n.name')
        assert sorted(row['n.name']['@none'] for row in rows) == ['Felix', 'Henry']

    def test_should_raise_on_invalid_gql_query(self) -> None:
        with pytest.raises(HttpStatusError) as exc_info:
            self.client.query(gql='MATCH (n:Cat RETURN n')
        assert exc_info.value.status_code == 400
        assert 'Syntax error' in exc_info.value.errors[0]

    def test_should_raise_when_page_size_exceeds_the_result_window(self) -> None:
        with pytest.raises(HttpStatusError) as exc_info:
            self.client.query_all(filters='type:Cat', page_size=1_000_000)
        assert exc_info.value.status_code == 400


class TestRejectedBatchIsolation:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        self.client = get_client('integration-testing', batch_size=16)
        self.client.tear_down(drop_datasets=False)
        self.client.apply_schema(get_schema())
        self.client.apply_datasets(get_datasets())

    def cats(self, invalid: set[int]) -> list[dict]:
        return [{'id': f'urn:pydg:Cat:iso{i}', 'type': 'Cat', 'name': f'cat{i}', 'naps': 'lots' if i in invalid else i}
                for i in range(16)]

    @pytest.mark.parametrize('isolation', [ISOLATION_MODE.BISECT, ISOLATION_MODE.INDIVIDUAL])
    def test_should_write_every_valid_entity_and_report_each_invalid_one(self, isolation) -> None:
        result = self.client.put('pets', self.cats({3, 11}), isolation=isolation)
        assert result.loaded == 14
        assert [f.index for f in result.failed] == [3, 11]
        for failure in result.failed:
            assert failure.status_code == 400
            assert failure.errors == ['Row 1: "naps" must be a number']
        assert sorted(c['id'] for c in self.client.get(class_name='Cat')) == sorted(
            f'urn:pydg:Cat:iso{i}' for i in range(16) if i not in {3, 11})

