import logging
import os
from enum import Enum
import copy

from udata_search_service.domain.entities import Dataset, Organization, Reuse
from udata_search_service.infrastructure.utils import get_concat_title_org, log2p, mdstrip


CONSUMER_LOGGING_LEVEL = int(os.environ.get("CONSUMER_LOGGING_LEVEL", logging.INFO))


class EventMessageType(Enum):
    INDEX = 'index'
    REINDEX = 'reindex'
    UNINDEX = 'unindex'


class DatasetConsumer(Dataset):
    @classmethod
    def load_from_dict(cls, data):
        # Strip markdown
        data["description"] = mdstrip(data["description"])

        organization = data["organization"]
        data["organization"] = organization.get('id') if organization else None
        data["orga_followers"] = organization.get('followers') if organization else None
        data["orga_sp"] = organization.get('public_service') if organization else None
        data["organization_name"] = organization.get('name') if organization else None

        data["concat_title_org"] = get_concat_title_org(data["title"], data['acronym'], data['organization_name'])
        data["geozones"] = [zone.get("id") for zone in data.get("geozones", [])]

        # Normalize values
        data["views"] = log2p(data.get("views", 0))
        data["followers"] = log2p(data.get("followers", 0))
        data["reuses"] = log2p(data.get("reuses", 0))
        data["orga_followers"] = log2p(data.get("orga_followers", 0))
        data["orga_sp"] = 4 if data.get("orga_sp", 0) else 1
        data["featured"] = 4 if data.get("featured", 0) else 1

        return super().load_from_dict(data)


class ReuseConsumer(Reuse):
    @classmethod
    def load_from_dict(cls, data):
        # Strip markdown
        data["description"] = mdstrip(data["description"])

        organization = data["organization"]
        data["organization"] = organization.get('id') if organization else None
        data["orga_followers"] = organization.get('followers') if organization else None
        data["organization_name"] = organization.get('name') if organization else None

        # Normalize values
        data["views"] = log2p(data.get("views", 0))
        data["followers"] = log2p(data.get("followers", 0))
        data["orga_followers"] = log2p(data.get("orga_followers", 0))
        return super().load_from_dict(data)


class OrganizationConsumer(Organization):
    @classmethod
    def load_from_dict(cls, data):
        # Strip markdown
        data["description"] = mdstrip(data["description"])

        data["followers"] = log2p(data.get("followers", 0))
        data["views"] = log2p(data.get("views", 0))
        return super().load_from_dict(data)


def parse_message(message: dict):
    value = copy.deepcopy(message)
    try:
        index_name = value.get("index")
        model, message_type = value["message_type"].split('.')
        if model == 'dataset':
            dataclass_consumer = DatasetConsumer
        elif model == 'reuse':
            dataclass_consumer = ReuseConsumer
        elif model == 'organization':
            dataclass_consumer = OrganizationConsumer
        else:
            raise ValueError(f'Model Deserializer not implemented for model: {model}')

        if not value.get("document"):
            document = None
        else:
            document = dataclass_consumer.load_from_dict(value.get("value")).to_dict()
        return message_type, index_name, document
    except Exception as e:
        raise ValueError(f'Failed to parse message: {value}. Exception raised: {e}')
