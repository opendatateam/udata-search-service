from unittest import mock


def test_search(app, client, single_dataset):
    dataset_service = mock.Mock()
    dataset_service.search.return_value = ([single_dataset], 1, 1)

    with app.container.dataset_service.override(dataset_service):
        response = client.get('api/1/datasets/?q=test')

    assert response.status_code == 200
    response_dataset = response.json['data'][0]
    assert single_dataset.title == response_dataset['title']
    assert single_dataset.description == response_dataset['description']
    assert single_dataset.url == response_dataset['url']
    assert single_dataset.id == response_dataset['id']
    assert single_dataset.es_dataset_featured == response_dataset['es_dataset_featured']


def test_get_dataset(app, client, single_dataset):
    dataset_service = mock.Mock()
    dataset_service.find_one.return_value = single_dataset

    with app.container.dataset_service.override(dataset_service):
        response = client.get('api/1/datasets/testid/')

    assert response.status_code == 200
    response_dataset = response.json
    assert single_dataset.title == response_dataset['title']
    assert single_dataset.description == response_dataset['description']
    assert single_dataset.url == response_dataset['url']
    assert single_dataset.id == response_dataset['id']
    assert single_dataset.es_dataset_featured == response_dataset['es_dataset_featured']
