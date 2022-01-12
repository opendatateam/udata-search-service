from app.infrastructure.services import DatasetService


def test_dataset_service_search(single_dataset, search_client):
    dataset_service = DatasetService(search_client=search_client)

    results, results_number, total_pages = dataset_service.search('test', 1, 20)
    assert len(results) == 3
    assert results_number == 3
    assert total_pages == 1


def test_dataset_service_find_one(single_dataset, search_client):
    dataset_service = DatasetService(search_client=search_client)

    result = dataset_service.find_one('test_id')
    assert result.title == single_dataset.title
    assert result.description == single_dataset.description
    assert result.id == single_dataset.id
    assert result.url == single_dataset.url
