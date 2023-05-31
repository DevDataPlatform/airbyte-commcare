import requests
from airbyte_cdk.sources import Source
from airbyte_cdk.models import SyncModeInfo, AirbyteCatalog, AirbyteMessage, Type


class AvniConnector(Source):
    def __init__(self):
        super().__init__()

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
           
            response = requests.get("https://avni-api.com/test", headers={"Authorization": config["api_token"]})
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def discover(self, logger, config) -> AirbyteCatalog:
        streams = []
        try:
            
            response = requests.get("https://avni-api.com/data_streams", headers={"Authorization": config["api_token"]})
            response.raise_for_status()
            avni_streams = response.json()

            
            for avni_stream in avni_streams:
                stream_name = avni_stream["name"]
                stream_schema = avni_stream["schema"]
                stream_replication_method = avni_stream["replication_method"]

                
                airbyte_stream = AirbyteStream(
                    name=stream_name,
                    json_schema=stream_schema,
                    supported_sync_modes=[stream_replication_method],
                )

             
                streams.append(airbyte_stream)

        except Exception as e:
           
            logger.error(f"Error during schema discovery: {str(e)}")

        
        return AirbyteCatalog(streams=streams)

    def read(self, logger, config, catalog, state=None) -> AirbyteMessage:
      
        data = [
            {"id": 1, "name": "John Doe", "email": "john.doe@example.com"},
            {"id": 2, "name": "Jane Smith", "email": "jane.smith@example.com"},
        ]

        stream_name = catalog.streams[0].name
        for record in data:
            
            message = AirbyteMessage(type=Type.RECORD, record=record, stream=stream_name)
            yield message

    def __call__(self, config_path, *args, **kwargs) -> Tuple[SyncModeInfo, AirbyteCatalog]:
        config = self.read_config(config_path)
        catalog = self.read_catalog(config_path)
        state = self.read_state(config_path)

        sync_mode = SyncModeInfo(sync_mode=SyncMode.full_refresh)

        return sync_mode, catalog


if __name__ == "__main__":
    source = AvniConnector()
    for message in source.read(logger=None, config={}, catalog={}):
        print(message)
